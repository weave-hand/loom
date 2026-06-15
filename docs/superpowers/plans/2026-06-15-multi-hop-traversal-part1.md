# Multi-Hop Traversal — Part 1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a governed multi-hop traversal read — follow an ordered chain of links (`Customer → Order → LineItem`) from source objects, returning the deduped final-target objects, with **every hop governed** (Read on each type; each type's row-filters applied inside the join).

**Architecture:** A new owned `compile_chain` SQL compiler emits a chain of INNER JOINs from the final target back to the source, reusing slice A's FK/join-table join shapes per hop, projecting only the final target and AND-ing every hop type's row-filters into the WHERE. A new `read_linked_chain` handler resolves the chain, gates `Read` on every hop type, loads every hop's policy, and serves the deduped target through the existing typed-JSON path. Single-hop traversal becomes the `N=1` case: `compile_traversal` is replaced by `compile_chain`, and `read_linked_objects` becomes a thin wrapper delegating to `read_linked_chain` (so the existing single-hop route and e2e are preserved). A new `GET /objects/{from}/links?path=l1,l2` route is added.

**Tech Stack:** Rust 2024, buck2, axum, DuckDB-over-DuckLake serving, `loom_fixture_test`. Tests are `rust_test`/`loom_fixture_test` targets — never inline `#[cfg(test)]`.

**Spec:** `docs/superpowers/specs/2026-06-15-query-multi-hop-traversal-design.md`

**Key design choices (baked in):**
- Chain types `[t_0 … t_k]` (t_0 = source, t_k = final target); hop `i` (1-based) connects `t_{i-1}` (from) → `t_i` (to). Aliases `t_0…t_k`; join-table mapping aliases `j1…jk`.
- Per-hop join predicate reuses slice A's `LinkBacking` shapes: FK → `t_{i-1}."<from_column>" = t_i."<to_column>"`; JoinTable → mapping `ji` joined in.
- Only the final target `t_k` is projected (masking/denied apply to it only). Every type's row-filters are AND'd into the WHERE — the leak-free guarantee.
- Param order: source eq-filters (bound to `t_0`) first, then every type's row-filters in chain order (`t_0`, `t_1`, … `t_k`).
- Depth cap = 4 hops. Reuse `quote_ident`/`filter_sql`/`MASK_MARKER`/`validate_row_filter` — no hand-rolled value interpolation.

---

## File Structure

| File | Responsibility | Action |
|------|----------------|--------|
| `src/services/query-api/src/sql.rs` | `ChainType` + `compile_chain`; remove `compile_traversal` | Modify |
| `src/services/query-api/tests/sql_compile.rs` | new `compile_chain` unit tests | Modify |
| `src/services/query-api/src/handler.rs` | `ChainQuery` + `read_linked_chain` + N-ends governance; `read_linked_objects` → wrapper; `BadChain` error | Modify |
| `src/services/query-api/src/http.rs` | new `?path=` route + `get_linked_chain`; `BadChain` → 400 | Modify |
| `src/services/query-api/tests/multi_hop_traversal_e2e.rs` | governed multi-hop e2e | Create |
| `src/services/query-api/BUCK` | e2e target | Modify |
| roadmap, `docs/FUTURE.md` | delivered marker + follow-ups | Modify |

---

## Task 1: `compile_chain` SQL compiler (alongside `compile_traversal`)

Add the owned chain compiler + unit tests. `compile_traversal` stays for now (handler still calls it; removed in Task 2) so the library keeps compiling.

**Files:**
- Modify: `src/services/query-api/src/sql.rs`
- Modify: `src/services/query-api/tests/sql_compile.rs`

- [ ] **Step 1: Add `ChainType` + `compile_chain`**

In `src/services/query-api/src/sql.rs`, after `compile_traversal` (end of file), add:

```rust
/// One type in a traversal chain: its physical table and the ACL row-filters that
/// govern it. Every hop's row-filters are ANDed into the join — the chain is governed
/// at every type, not just its endpoints.
pub struct ChainType {
    pub table: TableRef,
    pub row_filters: Vec<RowFilter>,
}

/// Compile a governed multi-hop traversal. `types` is the chain `[t_0 .. t_k]`
/// (`t_0` = source, `t_k` = final target); `hops[i]` is the link backing connecting
/// `types[i]` (from) to `types[i+1]` (to). Only the final target is projected
/// (`allowed_cols`, `mask_cols` rendered as the marker). `source_eq_filters` bind to
/// the source `t_0`. Every type's row-filters are ANDed into the WHERE.
///
/// Precondition: `types.len() == hops.len() + 1` and `hops` is non-empty (`k >= 1`).
/// Mirrors `compile_traversal`: row filters are validated up front so the `filter_sql`
/// invariant arms cannot panic.
#[allow(clippy::too_many_arguments)]
pub fn compile_chain(
    types: &[ChainType],
    hops: &[LinkBacking],
    allowed_cols: &[String],
    mask_cols: &[String],
    source_eq_filters: &[(String, SqlValue)],
    limit: u32,
) -> Result<(String, Vec<SqlValue>), CompileError> {
    debug_assert_eq!(types.len(), hops.len() + 1, "chain types must be hops + 1");
    for t in types {
        for f in &t.row_filters {
            validate_row_filter(f, None).map_err(CompileError::MalformedFilter)?;
        }
    }
    let k = hops.len();
    let alias = |i: usize| format!("t_{i}");
    let tbl = |t: &TableRef| format!("{}.{}", quote_ident(&t.schema), quote_ident(&t.name));

    // Projection: final target `t_k` only.
    let final_alias = alias(k);
    let cols = allowed_cols
        .iter()
        .map(|c| {
            if mask_cols.iter().any(|m| m == c) {
                format!("'{MASK_MARKER}' AS {}", quote_ident(c))
            } else {
                format!("{final_alias}.{}", quote_ident(c))
            }
        })
        .collect::<Vec<_>>()
        .join(", ");

    // FROM final target, then JOIN each predecessor down to the source.
    let mut from = format!("{} {}", tbl(&types[k].table), final_alias);
    for i in (1..=k).rev() {
        let to_alias = alias(i);
        let from_alias = alias(i - 1);
        let from_tbl = tbl(&types[i - 1].table);
        match &hops[i - 1] {
            LinkBacking::ForeignKey {
                from_column,
                to_column,
            } => {
                from.push_str(&format!(
                    " JOIN {from_tbl} {from_alias} ON {from_alias}.{} = {to_alias}.{}",
                    quote_ident(from_column),
                    quote_ident(to_column),
                ));
            }
            LinkBacking::JoinTable {
                table,
                from_key,
                from_column,
                to_column,
                to_key,
            } => {
                let jt = tbl(table);
                let j = format!("j{i}");
                from.push_str(&format!(
                    " JOIN {jt} {j} ON {j}.{} = {to_alias}.{} JOIN {from_tbl} {from_alias} ON {from_alias}.{} = {j}.{}",
                    quote_ident(to_column),
                    quote_ident(to_key),
                    quote_ident(from_key),
                    quote_ident(from_column),
                ));
            }
        }
    }

    // WHERE: source eq-filters (`t_0`), then every type's row-filters in chain order.
    let mut params = Vec::new();
    let mut conjuncts: Vec<String> = Vec::new();
    let src_alias = alias(0);
    for (col, val) in source_eq_filters {
        conjuncts.push(format!("({src_alias}.{} = ?)", quote_ident(col)));
        params.push(val.clone());
    }
    for (i, t) in types.iter().enumerate() {
        let a = alias(i);
        for f in &t.row_filters {
            conjuncts.push(filter_sql(f, &a, &mut params));
        }
    }

    let mut sql = format!("SELECT DISTINCT {cols} FROM {from}");
    if !conjuncts.is_empty() {
        sql.push_str(" WHERE ");
        sql.push_str(&conjuncts.join(" AND "));
    }
    sql.push_str(&format!(" LIMIT {limit}"));
    Ok((sql, params))
}
```

(`RowFilter`, `LinkBacking`, `TableRef`, `validate_row_filter` are already imported in `sql.rs` (line 7); `SqlValue`, `MASK_MARKER`, `quote_ident`, `filter_sql` are in-module. If any is not in scope, add it — match the existing imports.)

- [ ] **Step 2: Write the failing unit tests**

Append to `src/services/query-api/tests/sql_compile.rs` (import `compile_chain`, `ChainType` from `query_api::sql`, and `LinkBacking`, `TableRef`, `RowFilter`, `CompareOp`, `ScalarValue` from `control_plane_core` as needed — match the file's existing import style; `SqlValue` is already used in this file):

```rust
fn t(schema: &str, name: &str) -> TableRef {
    TableRef { schema: schema.into(), name: name.into() }
}

#[test]
fn chain_two_hop_fk_compiles_to_nested_joins() {
    // Customer --orders(FK id=customer_id)--> Order --lineItems(FK id=order_id)--> LineItem
    let types = vec![
        ChainType { table: t("main", "customer"), row_filters: vec![] },
        ChainType { table: t("main", "orders"), row_filters: vec![] },
        ChainType { table: t("main", "line_items"), row_filters: vec![] },
    ];
    let hops = vec![
        LinkBacking::ForeignKey { from_column: "id".into(), to_column: "customer_id".into() },
        LinkBacking::ForeignKey { from_column: "id".into(), to_column: "order_id".into() },
    ];
    let (sql, params) = compile_chain(
        &types,
        &hops,
        &["id".to_string(), "sku".to_string()],
        &[],
        &[("region".to_string(), SqlValue::Text("CA".into()))],
        100,
    )
    .unwrap();
    assert_eq!(
        sql,
        "SELECT DISTINCT t_2.\"id\", t_2.\"sku\" FROM \"main\".\"line_items\" t_2 \
         JOIN \"main\".\"orders\" t_1 ON t_1.\"id\" = t_2.\"order_id\" \
         JOIN \"main\".\"customer\" t_0 ON t_0.\"id\" = t_1.\"customer_id\" \
         WHERE (t_0.\"region\" = ?) LIMIT 100"
    );
    assert_eq!(params, vec![SqlValue::Text("CA".into())]);
}

#[test]
fn chain_fk_then_jointable_adds_mapping_join_for_that_hop_only() {
    // Customer --orders(FK)--> Order --tags(join-table order_tag)--> Tag
    let types = vec![
        ChainType { table: t("main", "customer"), row_filters: vec![] },
        ChainType { table: t("main", "orders"), row_filters: vec![] },
        ChainType { table: t("main", "tags"), row_filters: vec![] },
    ];
    let hops = vec![
        LinkBacking::ForeignKey { from_column: "id".into(), to_column: "customer_id".into() },
        LinkBacking::JoinTable {
            table: t("main", "order_tag"),
            from_key: "id".into(),
            from_column: "order_id".into(),
            to_column: "tag_id".into(),
            to_key: "id".into(),
        },
    ];
    let (sql, params) = compile_chain(&types, &hops, &["name".to_string()], &[], &[], 100).unwrap();
    assert_eq!(
        sql,
        "SELECT DISTINCT t_2.\"name\" FROM \"main\".\"tags\" t_2 \
         JOIN \"main\".\"order_tag\" j2 ON j2.\"tag_id\" = t_2.\"id\" \
         JOIN \"main\".\"orders\" t_1 ON t_1.\"id\" = j2.\"order_id\" \
         JOIN \"main\".\"customer\" t_0 ON t_0.\"id\" = t_1.\"customer_id\" LIMIT 100"
    );
    assert!(params.is_empty());
}

#[test]
fn chain_params_source_eq_precedes_hop_row_filters_in_chain_order() {
    // Source eq-filter param must precede an intermediate row-filter param.
    let types = vec![
        ChainType { table: t("main", "customer"), row_filters: vec![] },
        ChainType {
            table: t("main", "orders"),
            row_filters: vec![RowFilter::Compare {
                property: "status".into(),
                op: CompareOp::Eq,
                value: ScalarValue::Text("shipped".into()),
            }],
        },
        ChainType { table: t("main", "line_items"), row_filters: vec![] },
    ];
    let hops = vec![
        LinkBacking::ForeignKey { from_column: "id".into(), to_column: "customer_id".into() },
        LinkBacking::ForeignKey { from_column: "id".into(), to_column: "order_id".into() },
    ];
    let (sql, params) = compile_chain(
        &types,
        &hops,
        &["id".to_string()],
        &[],
        &[("region".to_string(), SqlValue::Text("CA".into()))],
        100,
    )
    .unwrap();
    assert_eq!(
        sql,
        "SELECT DISTINCT t_2.\"id\" FROM \"main\".\"line_items\" t_2 \
         JOIN \"main\".\"orders\" t_1 ON t_1.\"id\" = t_2.\"order_id\" \
         JOIN \"main\".\"customer\" t_0 ON t_0.\"id\" = t_1.\"customer_id\" \
         WHERE (t_0.\"region\" = ?) AND (t_1.\"status\" = ?) LIMIT 100"
    );
    assert_eq!(
        params,
        vec![SqlValue::Text("CA".into()), SqlValue::Text("shipped".into())]
    );
}

#[test]
fn chain_single_hop_reproduces_traversal_semantics() {
    // N=1: one FK hop, masked target column, source + target row filters.
    let types = vec![
        ChainType {
            table: t("main", "customer"),
            row_filters: vec![RowFilter::Compare {
                property: "region".into(),
                op: CompareOp::Eq,
                value: ScalarValue::Text("CA".into()),
            }],
        },
        ChainType { table: t("main", "orders"), row_filters: vec![] },
    ];
    let hops = vec![LinkBacking::ForeignKey {
        from_column: "id".into(),
        to_column: "customer_id".into(),
    }];
    let (sql, params) = compile_chain(
        &types,
        &hops,
        &["id".to_string(), "secret".to_string()],
        &["secret".to_string()],
        &[],
        100,
    )
    .unwrap();
    assert_eq!(
        sql,
        "SELECT DISTINCT t_1.\"id\", '***' AS \"secret\" FROM \"main\".\"orders\" t_1 \
         JOIN \"main\".\"customer\" t_0 ON t_0.\"id\" = t_1.\"customer_id\" \
         WHERE (t_0.\"region\" = ?) LIMIT 100"
    );
    assert_eq!(params, vec![SqlValue::Text("CA".into())]);
}
```

- [ ] **Step 3: Run + lint**

Run (serial): `buck2 test //src/services/query-api:sql-compile > /tmp/t1.log 2>&1; grep -E "Tests finished|FAIL|panicked|left|right" /tmp/t1.log` → all pass (existing + 4 new).
Run: `buck2 build '//src/services/query-api:query-api[clippy.txt]' 2>&1 | tail -5` → empty.

If a string assertion fails on a formatting difference vs the real `filter_sql`/`quote_ident` output, fix the EXPECTED string to the real output (do not change the compiler) — provided the SQL is correct, injection-safe, with the right alias/param order.

- [ ] **Step 4: Commit**

```bash
git add src/services/query-api/src/sql.rs src/services/query-api/tests/sql_compile.rs
git commit -m "feat(query-api): compile_chain governed multi-hop traversal SQL"
```

---

## Task 2: `read_linked_chain` handler + N-ends governance

Resolve the chain, gate Read on every hop type, build `compile_chain` inputs, serve. Make `read_linked_objects` a thin wrapper (preserves the single-hop e2e). Remove `compile_traversal`.

**Files:**
- Modify: `src/services/query-api/src/handler.rs`
- Modify: `src/services/query-api/src/sql.rs` (remove `compile_traversal`)

- [ ] **Step 1: Add the `BadChain` error variant**

In `src/services/query-api/src/handler.rs`, READ the `QueryError` enum (around lines 42-59) and add a variant for a malformed chain shape (empty path / depth over cap). Mirror the existing variants' style:

```rust
    /// The traversal chain is malformed (empty path, or depth over the cap).
    BadChain(String),
```

(If `QueryError` derives `thiserror::Error` with `#[error(...)]` messages, add a matching `#[error("bad chain: {0}")]`. Match the real derive/attribute style of the neighboring variants exactly.)

- [ ] **Step 2: Add `ChainQuery` + depth cap + `read_linked_chain`**

In `handler.rs`, near `LinkQuery`/`read_linked_objects`, add the chain query type, the cap, and the resolver. READ the real `read_linked_objects` (lines 270-376), `load_policy` (61-87), and `project_allowed` (90-99) first; this generalizes them. `ObjectType` must be importable — add `ObjectType` to the `use control_plane_core::{…}` import if not already present.

```rust
/// Maximum chain depth (number of hops). A request beyond this is rejected before
/// any catalog/ACL work — bounds the join count.
const MAX_CHAIN_DEPTH: usize = 4;

/// A governed multi-hop traversal: from source objects matching `source_filters`,
/// follow `path` (an ordered list of link names), return the deduped final-target
/// objects. Every type in the chain is governed (Read + row-filters).
pub struct ChainQuery {
    pub from_type: String,
    pub path: Vec<String>,
    pub source_filters: Vec<(String, SqlValue)>,
}

pub async fn read_linked_chain(
    q: &ChainQuery,
    subject: &Subject,
    deps: &QueryDeps<'_>,
) -> Result<ObjectRows, QueryError> {
    if q.path.is_empty() || q.path.len() > MAX_CHAIN_DEPTH {
        return Err(QueryError::BadChain(format!(
            "path length {} (allowed 1..={MAX_CHAIN_DEPTH})",
            q.path.len()
        )));
    }

    let from_name = TypeName(q.from_type.clone());
    let from_target = PolicyTarget::Type(from_name.clone());
    // Read on the source (deny-by-default, before existence is revealed).
    if deps.acl.check(&subject.0, Action::Read, &from_target).await? == Decision::Deny {
        return Err(QueryError::Forbidden);
    }
    let from_type = deps
        .ontology
        .get_type(&from_name)
        .await
        .map_err(|e| match e {
            ControlPlaneError::NotFound(_) => QueryError::UnknownType(q.from_type.clone()),
            other => QueryError::ControlPlane(other),
        })?;
    let (s_filters, s_denied, s_masked) =
        load_policy(deps.acl, &subject.0, &from_target).await?;

    // Resolve the chain left-to-right, gating Read on every hop type and loading each
    // type's policy. `ctypes[0]` = source; `ctypes[k]` = final target.
    let mut ctypes: Vec<crate::sql::ChainType> = vec![crate::sql::ChainType {
        table: from_type.table.clone(),
        row_filters: s_filters,
    }];
    let mut hops: Vec<control_plane_core::LinkBacking> = Vec::with_capacity(q.path.len());
    // Final-target accumulators (always overwritten: path is non-empty).
    let mut target_type: ObjectType = from_type.clone();
    let mut target_denied = std::collections::HashSet::new();
    let mut target_masked = std::collections::HashSet::new();
    let mut current_name = from_name.clone();
    for link_name in &q.path {
        let links = deps
            .ontology
            .links(&current_name, PageReq::unbounded())
            .await
            .map_err(|e| match e {
                ControlPlaneError::NotFound(_) => QueryError::UnknownType(current_name.0.clone()),
                other => QueryError::ControlPlane(other),
            })?;
        let link = links
            .items
            .into_iter()
            .find(|l| &l.name == link_name)
            .ok_or_else(|| QueryError::UnknownLink(link_name.clone()))?;
        let to_name = link.to.clone();
        let to_target = PolicyTarget::Type(to_name.clone());
        // Read on every hop type (the leak-free guarantee: no traversing through a
        // type the subject cannot read).
        if deps.acl.check(&subject.0, Action::Read, &to_target).await? == Decision::Deny {
            return Err(QueryError::Forbidden);
        }
        // A link pointing at a missing type is an internal inconsistency, not a 404.
        let to_type = deps.ontology.get_type(&to_name).await?;
        let (t_filters, t_denied, t_masked) =
            load_policy(deps.acl, &subject.0, &to_target).await?;
        hops.push(link.backing.clone());
        ctypes.push(crate::sql::ChainType {
            table: to_type.table.clone(),
            row_filters: t_filters,
        });
        target_type = to_type;
        target_denied = t_denied;
        target_masked = t_masked;
        current_name = to_name;
    }

    // Source eq-filter columns must be visible (allowed, non-masked) on the source.
    let from_allowed = project_allowed(&from_type.properties, &s_denied);
    for (col, _) in &q.source_filters {
        if !from_allowed.contains(col) || s_masked.contains(col) {
            return Err(QueryError::BadFilter(col.clone()));
        }
    }

    // Final-target projection.
    let to_allowed = project_allowed(&target_type.properties, &target_denied);
    if to_allowed.is_empty() {
        return Err(QueryError::Forbidden);
    }
    let to_mask_cols: Vec<String> = to_allowed
        .iter()
        .filter(|c| target_masked.contains(*c))
        .cloned()
        .collect();

    let (sql, params) = compile_chain(
        &ctypes,
        &hops,
        &to_allowed,
        &to_mask_cols,
        &q.source_filters,
        DEFAULT_LIMIT,
    )?;
    let served = deps.serving.fetch_rows(&sql, &params).await?;
    let logical_types: Vec<String> = to_allowed
        .iter()
        .map(|name| {
            target_type
                .properties
                .iter()
                .find(|p| &p.name == name)
                .map(|p| p.ty.clone())
                .unwrap_or_default()
        })
        .collect();
    debug_assert_eq!(
        served.columns, to_allowed,
        "serving engine returned columns out of the projected order"
    );
    Ok(ObjectRows {
        columns: to_allowed,
        logical_types,
        rows: served.rows,
    })
}
```

(`compile_chain` must be imported: change `use crate::sql::{compile_select, compile_traversal};` to `use crate::sql::{compile_chain, compile_select};`. `ObjectType`, `LinkBacking` from `control_plane_core` — add to imports if missing. `std::collections::HashSet` is referenced fully-qualified like the rest of the file.)

- [ ] **Step 3: Make `read_linked_objects` a thin wrapper**

Replace the entire body of `read_linked_objects` (lines 270-376) with a delegation (keep `LinkQuery` and the `pub async fn read_linked_objects` signature unchanged — the `link_traversal.rs` e2e calls them directly):

```rust
pub async fn read_linked_objects(
    q: &LinkQuery,
    subject: &Subject,
    deps: &QueryDeps<'_>,
) -> Result<ObjectRows, QueryError> {
    read_linked_chain(
        &ChainQuery {
            from_type: q.from_type.clone(),
            path: vec![q.link.clone()],
            source_filters: q.source_filters.clone(),
        },
        subject,
        deps,
    )
    .await
}
```

- [ ] **Step 4: Remove `compile_traversal`**

In `src/services/query-api/src/sql.rs`, delete the `compile_traversal` function (lines ~295-390) and its doc comment — it is now unused (handler delegates to `compile_chain`). Confirm nothing else references it: `grep -rn compile_traversal src/services/query-api/`. (Only this deletion should remain; if the `sql_compile.rs` tests referenced it they would already be gone — they never did.)

- [ ] **Step 5: Build + lint**

Run (serial): `buck2 build //src/services/query-api:query-api 2>&1 | tail -10` (clean — no `compile_traversal` references remain).
Run: `buck2 build '//src/services/query-api:query-api[clippy.txt]' 2>&1 | tail -5` (empty).
Run the single-hop e2e (the regression guard — it calls `read_linked_objects` directly and must stay green, proving single-hop behavior is preserved through `compile_chain`):
`buck2 test //src/services/query-api:link-traversal > /tmp/t2.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t2.log` → `Pass 5. Fail 0.`

- [ ] **Step 6: Commit**

```bash
git add src/services/query-api/src/handler.rs src/services/query-api/src/sql.rs
git commit -m "feat(query-api): read_linked_chain N-ends-governed multi-hop traversal"
```

---

## Task 3: HTTP `?path=` route

Add the new route; map `BadChain` → 400. The existing single-hop route is unchanged (it delegates through the wrapper).

**Files:**
- Modify: `src/services/query-api/src/http.rs`

- [ ] **Step 1: Add the route + handler**

In `src/services/query-api/src/http.rs`:

Add `read_linked_chain, ChainQuery` to the `use crate::handler::{…}` import.

Register the new route in `router` (after the existing `links/:link_name` route):

```rust
        .route("/objects/:from_type/links", get(get_linked_chain))
```

Add the handler (mirror `get_linked`, but parse `path` from the query string and exclude it from source filters):

```rust
async fn get_linked_chain(
    State(st): State<AppState>,
    Path(from_type): Path<String>,
    Query(mut params): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> impl IntoResponse {
    let subject = headers
        .get("X-Loom-Subject")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("anonymous")
        .to_string();
    // `path` is the comma-separated ordered chain of link names; everything else is a
    // source eq-filter. A request with no usable `path` is a malformed chain (-> 400).
    let path: Vec<String> = params
        .remove("path")
        .unwrap_or_default()
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();
    // Slice limitation: every query-param filter binds as Text (typed filters later).
    let source_filters = params
        .into_iter()
        .map(|(k, v)| (k, SqlValue::Text(v)))
        .collect();
    let deps = QueryDeps {
        ontology: st.cp.ontology(),
        acl: st.cp.acl(),
        serving: st.serving.as_ref(),
    };
    match read_linked_chain(
        &ChainQuery {
            from_type,
            path,
            source_filters,
        },
        &Subject(SubjectId(subject)),
        &deps,
    )
    .await
    {
        Ok(rows) => Json(crate::render::objects_to_json(&rows)).into_response(),
        Err(QueryError::UnknownType(t)) => (StatusCode::NOT_FOUND, t).into_response(),
        Err(QueryError::UnknownLink(l)) => (StatusCode::NOT_FOUND, l).into_response(),
        Err(QueryError::BadChain(m)) => (StatusCode::BAD_REQUEST, m).into_response(),
        Err(QueryError::Forbidden) => StatusCode::FORBIDDEN.into_response(),
        Err(QueryError::BadFilter(c)) => (StatusCode::BAD_REQUEST, c).into_response(),
        Err(_) => (StatusCode::INTERNAL_SERVER_ERROR, "internal error").into_response(),
    }
}
```

- [ ] **Step 2: Map `BadChain` in the existing `get_linked` handler too**

In `get_linked`'s match arms, add a `BadChain` arm (the wrapper can now surface it, e.g. for a future empty single link — keep the surface consistent):

```rust
        Err(QueryError::BadChain(m)) => (StatusCode::BAD_REQUEST, m).into_response(),
```

- [ ] **Step 3: Build + lint**

Run (serial): `buck2 build //src/services/query-api:query-api-bin 2>&1 | tail -10` (clean).
Run: `buck2 build '//src/services/query-api:query-api[clippy.txt]' 2>&1 | tail -5` (empty).
Run the HTTP smoke test (no regression): `buck2 test //src/services/query-api:http-smoke > /tmp/t3.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t3.log` → pass.

- [ ] **Step 4: Commit**

```bash
git add src/services/query-api/src/http.rs
git commit -m "feat(query-api): GET /objects/{from}/links?path= multi-hop route"
```

---

## Task 4: Governed multi-hop e2e

**Files:**
- Create: `src/services/query-api/tests/multi_hop_traversal_e2e.rs`
- Modify: `src/services/query-api/BUCK`

- [ ] **Step 1: Write the e2e**

Create `src/services/query-api/tests/multi_hop_traversal_e2e.rs`. **Mirror `tests/link_traversal.rs` EXACTLY** for the fixture/seeding/ACL/serving setup (`PgFixture`, the ingest-materializer `land` helper, `EmbeddedDuckDb::attach`, `QueryDeps`, the `define_subject`/`define_role`/`assign_role`/`grant`/`set_policy` API). READ `link_traversal.rs` and `derived_properties_e2e.rs` first and reproduce their setup verbatim; the assertions below are the contract.

Seed three tables forming an FK chain:
- `customer(id Long, region String)`: `(1,'CA')`, `(2,'NY')`.
- `orders(id Long, customer_id Long, status String)`: `(10,1,'shipped')`, `(11,1,'pending')`, `(20,2,'shipped')`.
- `line_items(id Long, order_id Long, sku String)`: `(100,10,'A')`, `(101,10,'B')`, `(102,11,'C')`, `(200,20,'D')`.

Ontology: types `Customer`, `Order`, `LineItem` (each `derived: vec![]`); FK links `orders` (`Customer→Order`, `ForeignKey{from_column:"id", to_column:"customer_id"}`) and `lineItems` (`Order→LineItem`, `ForeignKey{from_column:"id", to_column:"order_id"}`).

```rust
//! Multi-hop traversal e2e: Customer -> Order -> LineItem, governed at every hop.
//! Served + correct; an intermediate Order row-filter narrows the reachable LineItems;
//! denying Read on the intermediate Order type forbids the whole traversal.

// ... imports + fixture/seed/ACL setup mirrored from link_traversal.rs ...
// Produces `cp`, the `EmbeddedDuckDb` engine `eng`, and seeded data as above.

#[tokio::test(flavor = "multi_thread")]
async fn multi_hop_served_and_governed() {
    // ---- fixture + seed (mirror link_traversal.rs): customer, orders, line_items ----
    // ---- ontology: 3 types + 2 FK links (as specified above) ----

    let customer = TypeName("Customer".into());
    let order = TypeName("Order".into());
    let line_item = TypeName("LineItem".into());

    // ---- subject A: Read on all three -> sees the reachable LineItems ----
    // grant Read on Customer, Order, LineItem to A.
    let deps = QueryDeps { ontology: &cp, acl: &cp, serving: &eng };
    let rows = read_linked_chain(
        &ChainQuery {
            from_type: "Customer".into(),
            path: vec!["orders".into(), "lineItems".into()],
            source_filters: vec![("region".into(), SqlValue::Text("CA".into()))],
        },
        &Subject(a.clone()),
        &deps,
    )
    .await
    .unwrap();
    let body = objects_to_json(&rows);
    let mut ids: Vec<String> = body["objects"].as_array().unwrap().iter()
        .map(|o| o["id"].as_str().unwrap().to_string())
        .collect();
    ids.sort();
    // Customer 1 (CA) -> orders 10,11 -> line_items 100,101 (order 10) + 102 (order 11).
    assert_eq!(ids, vec!["100".to_string(), "101".to_string(), "102".to_string()]);

    // ---- subject C: Read on all three + Order row-filter status='shipped' ----
    // grant Read on all three to C; set_policy on Order with row_filter status='shipped'.
    let rows_c = read_linked_chain(
        &ChainQuery {
            from_type: "Customer".into(),
            path: vec!["orders".into(), "lineItems".into()],
            source_filters: vec![("region".into(), SqlValue::Text("CA".into()))],
        },
        &Subject(c.clone()),
        &deps,
    )
    .await
    .unwrap();
    let body_c = objects_to_json(&rows_c);
    let mut ids_c: Vec<String> = body_c["objects"].as_array().unwrap().iter()
        .map(|o| o["id"].as_str().unwrap().to_string())
        .collect();
    ids_c.sort();
    // Only shipped order 10 is traversable -> line_items 100,101 (102 via pending order 11 dropped).
    assert_eq!(ids_c, vec!["100".to_string(), "101".to_string()]);

    // ---- subject B: Read on Customer + LineItem but NOT Order -> 403 ----
    // grant Read on Customer and LineItem only to B.
    let err = read_linked_chain(
        &ChainQuery {
            from_type: "Customer".into(),
            path: vec!["orders".into(), "lineItems".into()],
            source_filters: vec![],
        },
        &Subject(b.clone()),
        &deps,
    )
    .await
    .unwrap_err();
    assert!(matches!(err, QueryError::Forbidden), "no Read on intermediate Order -> Forbidden");
}
```

Import `read_linked_chain`, `ChainQuery`, `Subject`, `QueryDeps`, `QueryError` from `query_api::handler`; `objects_to_json` from `query_api::render`; `EmbeddedDuckDb` from `query_api::serving`; the ontology/ACL types from `control_plane_core`; fixture from `control_plane_postgres::fixture`. Confirm the FK link `from_column`/`to_column` semantics by checking how `link_traversal.rs` seeds + defines its FK link (same direction). Do NOT weaken the three assertions (served+correct, mid-hop narrows, intermediate-Read-denied → 403).

- [ ] **Step 2: BUCK target**

Add to `src/services/query-api/BUCK`, copying the EXACT deps/attrs of the neighboring `link-traversal` `loom_fixture_test` target (same dependency surface):

```python
loom_fixture_test(
    name = "multi-hop-traversal-e2e",
    crate = "multi_hop_traversal_e2e",
    srcs = ["tests/multi_hop_traversal_e2e.rs"],
    crate_root = "tests/multi_hop_traversal_e2e.rs",
    duckdb = True,
    deps = [
        ":query-api",
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

- [ ] **Step 3: Run + commit**

Run (serial): `buck2 test //src/services/query-api:multi-hop-traversal-e2e > /tmp/t4.log 2>&1; grep -E "Tests finished|FAIL|panicked|assertion|left|right" /tmp/t4.log` → `Pass 1. Fail 0.`
If the reachable set is wrong, check the FK directions (`t_1.id = t_2.order_id`, `t_0.id = t_1.customer_id`) and the seeded rows. Do NOT weaken assertions.

```bash
git add src/services/query-api/tests/multi_hop_traversal_e2e.rs src/services/query-api/BUCK
git commit -m "test(query-api): multi-hop traversal e2e — served + every-hop governed"
```

---

## Task 5: Docs

**Files:**
- Modify: `docs/superpowers/specs/2026-06-06-loom-roadmap.md`, `docs/FUTURE.md`

- [ ] **Step 1: Roadmap delivered marker**

In `docs/superpowers/specs/2026-06-06-loom-roadmap.md`, find the richer-reads entries (link traversal = slice A, derived properties = slice B, both delivered). Add a sibling **multi-hop traversal (slice C, part-1)** delivered entry matching the adjacent format: an ordered chain of links (`GET /objects/{from}/links?path=l1,l2`) served as a `SELECT DISTINCT` chain of governed INNER JOINs, governed at **every** hop (Read on each type; each type's row-filters applied inside the join), depth-capped, proven by an e2e. Reference `docs/superpowers/specs/2026-06-15-query-multi-hop-traversal-design.md`. Update the "Candidate next slices" line so multi-hop is no longer pending (the remaining slice-C parts are inverse / target-side filtering / object-set / association).

- [ ] **Step 2: FUTURE.md follow-ups**

In `docs/FUTURE.md`, "Ontology & read path (Step 3)" section: update the "remaining relational-read slices" note (slice A delivered, slice B delivered, **slice C part-1 (multi-hop) now delivered**; the remaining slice-C parts are the follow-ons). Add the multi-hop follow-ups (match the existing bold-lead + why-deferred style), sourced from `2026-06-15-query-multi-hop-traversal-design.md`:
- **Inverse-direction hops** — follow a link from its `to` back to its `from` within a chain.
- **Caller-supplied target / intermediate filters** — equality filters on types beyond the source (target-side filtering).
- **Object-set inputs** — start a chain from a passed/saved set of source object IDs.
- **Source→target association** — return which source each final target came from (ties into a visible-primary-key concept).
- **Define-time chain/link validation** — validate link continuity + physical columns at authoring time (shared with slice A's deferred `define_link` validation).
- **Derived properties on chain output** — serve slice B's aggregates on traversal/chain output.

- [ ] **Step 3: Commit**

```bash
git add docs/superpowers/specs/2026-06-06-loom-roadmap.md docs/FUTURE.md
git commit -m "docs: multi-hop traversal (slice C part 1) delivered; record follow-ups"
```

---

## Final Verification

- [ ] `buck2 build //src/... 2>&1 | tail -20` — clean.
- [ ] `buck2 test //src/... > /tmp/sweep.log 2>&1; grep -E "Tests finished|FAIL" /tmp/sweep.log` — `Fail 0`, including `sql-compile` (4 new), `multi-hop-traversal-e2e`, `link-traversal` (5, unchanged — single-hop preserved), `http-smoke`, and the other read tests.
- [ ] `tools/clippy-all.sh 2>&1 | tail -5` — clean.
- [ ] `git status` — no `Cargo.lock`/`third-party`/`.sqlx` drift (this slice touches no SQL cache or deps).
- [ ] **Run buck2 commands serially** — never a second buck2 invocation (or a commit whose hooks run buck2) while a `buck2 test //src/...` sweep runs.
- [ ] Markdown edits (Task 5) end with exactly one trailing newline, no trailing whitespace (the `lint` CI job checks all `.md`).

Then proceed to **superpowers:finishing-a-development-branch**.
