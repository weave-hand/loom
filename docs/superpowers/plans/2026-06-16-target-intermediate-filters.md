# Target / Intermediate Filters (slice C part-2) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let a query caller filter **any** type in a governed traversal chain (source, every intermediate, final target) by typed equality, addressed by the link that reaches each type — not just the source.

**Architecture:** The chain compiler (`compile_chain`) already aliases every type `t_0..t_k` and ANDs each one's ACL row-filters at its alias. This slice adds a caller `eq_filters` list per `ChainType`, bound at the same alias; the handler governs each caller filter against *its own* type (visibility-then-coerce, reusing `filter::coerce_filter`); and a small pure HTTP-side resolver maps `<linkname>.<column>` query keys to chain positions (bare keys = source), rejecting unknown prefixes and — the relational/graph boundary — per-hop filters on a link that repeats in the path.

**Tech Stack:** Rust 2024, buck2, axum (HTTP surface), DuckDB-over-DuckLake serving engine, hermetic Postgres+DuckDB fixture tests (`loom_fixture_test`).

**Spec:** `docs/superpowers/specs/2026-06-16-query-target-intermediate-filters-design.md`

**Conventions for every task:**
- Tests are integration `rust_test` / `loom_fixture_test` targets only — never inline `#[cfg(test)]`.
- **Never run two buck2 commands concurrently** (single daemon — they hang). Run them one at a time.
- Don't pipe `buck2 test` through `tail`/`head` (stalls). Redirect to a file and grep: `buck2 test //src/services/query-api:<tgt> > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t.log`.
- Commit at the end of each task. End commit messages with the `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>` trailer.
- Markdown files end with exactly one trailing newline, no trailing whitespace.

---

## File Structure

- `src/services/query-api/src/sql.rs` — **modify.** `ChainType` gains `eq_filters`; `compile_chain` drops the standalone `source_eq_filters` param and binds each type's caller eq-filters at `t_i`.
- `src/services/query-api/src/handler.rs` — **modify.** `ChainFilter` struct; `ChainQuery`/`LinkQuery` carry positioned `filters`; `read_linked_chain` retains per-position governance metadata and applies visibility-then-coerce per caller filter; `read_linked_objects` forwards.
- `src/services/query-api/src/chain_filter.rs` — **create.** Pure resolver `resolve_chain_filters(path, params) -> Result<Vec<ChainFilter>, FilterResolveError>` mapping wire keys to positions; the relational/graph boundary lives here.
- `src/services/query-api/src/lib.rs` — **modify.** Register `pub mod chain_filter;`.
- `src/services/query-api/src/http.rs` — **modify.** `get_linked` / `get_linked_chain` resolve keys via `resolve_chain_filters` and forward positioned filters; a resolver error → 400.
- `src/services/query-api/tests/sql_compile.rs` — **modify.** Update `ChainType` literals + `compile_chain` calls; add positioned-eq unit tests.
- `src/services/query-api/tests/chain_filter_resolve.rs` — **create.** Pure unit tests for the resolver (bare→source, prefix→position, unknown prefix, repeated link).
- `src/services/query-api/tests/link_traversal.rs` — **modify.** `LinkQuery` construction → positioned `filters`.
- `src/services/query-api/tests/multi_hop_traversal_e2e.rs` — **modify.** `ChainQuery` construction → positioned `filters`; add positioned-filter e2e cases.
- `src/services/query-api/BUCK` — **modify.** Add the `chain-filter-resolve` `rust_test` target.
- `docs/superpowers/specs/2026-06-06-loom-roadmap.md`, `docs/FUTURE.md` — **modify.** Delivered marker + graph-traversal defer item.

---

## Task 1: Compiler — per-position caller eq-filters in `compile_chain`

Add caller equality filters to every chain position (folding the source-only param away), keeping behavior identical for existing callers. This touches `sql.rs` and its single non-test caller (`handler.rs`) together because the lib must build as a unit; the handler change here is the minimal behavior-preserving adaptation (full positioned governance is Task 2).

**Files:**
- Modify: `src/services/query-api/src/sql.rs`
- Modify: `src/services/query-api/src/handler.rs` (one call site + `ChainType` literals)
- Test: `src/services/query-api/tests/sql_compile.rs`

- [ ] **Step 1: Add the new positioned-eq unit tests (failing).**

Append these two tests to `src/services/query-api/tests/sql_compile.rs` (they use the existing `tr` helper and `ChainType`/`compile_chain` imports already at the top of the file):

```rust
#[test]
fn chain_eq_filter_on_final_target_binds_at_t_k() {
    let types = vec![
        ChainType {
            table: tr("main", "customer"),
            row_filters: vec![],
            eq_filters: vec![],
        },
        ChainType {
            table: tr("main", "orders"),
            row_filters: vec![],
            eq_filters: vec![],
        },
        ChainType {
            table: tr("main", "line_items"),
            row_filters: vec![],
            eq_filters: vec![("sku".into(), SqlValue::Text("A".into()))],
        },
    ];
    let hops = vec![
        LinkBacking::ForeignKey {
            from_column: "id".into(),
            to_column: "customer_id".into(),
        },
        LinkBacking::ForeignKey {
            from_column: "id".into(),
            to_column: "order_id".into(),
        },
    ];
    let (sql, params) = compile_chain(&types, &hops, &["id".to_string()], &[], 100).unwrap();
    assert_eq!(
        sql,
        "SELECT DISTINCT t_2.\"id\" FROM \"main\".\"line_items\" t_2 \
         JOIN \"main\".\"orders\" t_1 ON t_1.\"id\" = t_2.\"order_id\" \
         JOIN \"main\".\"customer\" t_0 ON t_0.\"id\" = t_1.\"customer_id\" \
         WHERE (t_2.\"sku\" = ?) LIMIT 100"
    );
    assert_eq!(params, vec![SqlValue::Text("A".into())]);
}

#[test]
fn chain_eq_filters_bind_per_position_in_chain_order() {
    let types = vec![
        ChainType {
            table: tr("main", "customer"),
            row_filters: vec![],
            eq_filters: vec![("region".into(), SqlValue::Text("CA".into()))],
        },
        ChainType {
            table: tr("main", "orders"),
            row_filters: vec![],
            eq_filters: vec![("id".into(), SqlValue::Int(10))],
        },
        ChainType {
            table: tr("main", "line_items"),
            row_filters: vec![],
            eq_filters: vec![],
        },
    ];
    let hops = vec![
        LinkBacking::ForeignKey {
            from_column: "id".into(),
            to_column: "customer_id".into(),
        },
        LinkBacking::ForeignKey {
            from_column: "id".into(),
            to_column: "order_id".into(),
        },
    ];
    let (sql, params) = compile_chain(&types, &hops, &["id".to_string()], &[], 100).unwrap();
    assert_eq!(
        sql,
        "SELECT DISTINCT t_2.\"id\" FROM \"main\".\"line_items\" t_2 \
         JOIN \"main\".\"orders\" t_1 ON t_1.\"id\" = t_2.\"order_id\" \
         JOIN \"main\".\"customer\" t_0 ON t_0.\"id\" = t_1.\"customer_id\" \
         WHERE (t_0.\"region\" = ?) AND (t_1.\"id\" = ?) LIMIT 100"
    );
    assert_eq!(
        params,
        vec![SqlValue::Text("CA".into()), SqlValue::Int(10)]
    );
}
```

- [ ] **Step 2: Update existing `compile_chain` call sites in the test to the new signature.**

The `compile_chain` signature loses its `source_eq_filters` argument and every `ChainType` literal gains `eq_filters`. In `src/services/query-api/tests/sql_compile.rs`, apply these edits to the **existing** chain tests:

In `chain_two_hop_fk_compiles_to_nested_joins`: add `eq_filters: vec![]` to the `customer` and `line_items` `ChainType`s, set the `orders` one to `eq_filters: vec![]` as well, move the source filter into the `customer` (`t_0`) type, and drop the `&[("region"...)]` argument. The `customer` ChainType becomes:

```rust
        ChainType {
            table: tr("main", "customer"),
            row_filters: vec![],
            eq_filters: vec![("region".to_string(), SqlValue::Text("CA".into()))],
        },
```

and the call becomes:

```rust
    let (sql, params) = compile_chain(
        &types,
        &hops,
        &["id".to_string(), "sku".to_string()],
        &[],
        100,
    )
    .unwrap();
```

(The expected SQL/params assertions in that test are unchanged — `t_0."region" = ?` with param `CA`.)

In `chain_fk_then_jointable_adds_mapping_join_for_that_hop_only`, `chain_single_hop_jointable_renders_j1_mapping`, and `chain_single_hop_reproduces_traversal_semantics`: add `eq_filters: vec![]` to **every** `ChainType` literal, and remove the `&[]` source-filter argument from each `compile_chain(...)` call (so the call is `compile_chain(&types, &hops, &[...cols], &[...mask], 100)`).

In `chain_params_source_eq_precedes_hop_row_filters_in_chain_order`: add `eq_filters: vec![]` to the `orders` and `line_items` types, set the `customer` type's `eq_filters` to the moved source filter, keep its `row_filters: vec![]`, keep the `orders` `row_filters` (the `status='shipped'` compare), and drop the source-filter arg:

```rust
        ChainType {
            table: tr("main", "customer"),
            row_filters: vec![],
            eq_filters: vec![("region".to_string(), SqlValue::Text("CA".into()))],
        },
```

```rust
    let (sql, params) = compile_chain(&types, &hops, &["id".to_string()], &[], 100).unwrap();
```

(Its expected SQL `WHERE (t_0."region" = ?) AND (t_1."status" = ?)` and params `[CA, shipped]` are unchanged.)

- [ ] **Step 3: Run the test target to confirm it fails to compile.**

Run: `buck2 test //src/services/query-api:sql-compile > /tmp/t.log 2>&1; grep -E "error\[|cannot find|Tests finished|FAIL" /tmp/t.log`
Expected: compile errors — `ChainType` has no field `eq_filters`, and `compile_chain` takes a different number of arguments. (Proves the tests exercise the new shape.)

- [ ] **Step 4: Add `eq_filters` to `ChainType` and rebind in `compile_chain`.**

In `src/services/query-api/src/sql.rs`, change the `ChainType` struct (around line 297) to:

```rust
/// One type in a traversal chain: its physical table, the ACL row-filters that govern
/// it, and the caller equality filters bound at this position. Every position's filters
/// are ANDed at its alias `t_i` — the chain is governed and caller-filterable at every
/// type, not just its endpoints.
pub struct ChainType {
    pub table: TableRef,
    pub row_filters: Vec<RowFilter>,
    /// Caller equality filters (`col = value`) for this position, bound at alias `t_i`.
    /// Position 0's eq_filters are the source filters (no special-case in the compiler).
    pub eq_filters: Vec<(String, SqlValue)>,
}
```

Change the `compile_chain` signature (around line 311) to drop `source_eq_filters` and its `#[allow(clippy::too_many_arguments)]` (now 5 args, under the lint threshold). Update the doc comment's "`source_eq_filters` bind to the source `t_0`" sentence to "each type's `eq_filters` bind at its alias `t_i` (position 0 = source)":

```rust
/// Compile a governed multi-hop traversal. `types` is the chain `[t_0 .. t_k]`
/// (`t_0` = source, `t_k` = final target); `hops[i]` is the link backing connecting
/// `types[i]` (from) to `types[i+1]` (to). Only the final target is projected
/// (`allowed_cols`, `mask_cols` rendered as the marker). Each type's `eq_filters` bind
/// at its alias `t_i` (position 0 = source). Every type's row-filters are ANDed into the WHERE.
///
/// Precondition: `types.len() == hops.len() + 1` and `hops` is non-empty (`k >= 1`).
/// As in `compile_select`, row filters are validated up front so the `filter_sql`
/// invariant arms cannot panic.
pub fn compile_chain(
    types: &[ChainType],
    hops: &[LinkBacking],
    allowed_cols: &[String],
    mask_cols: &[String],
    limit: u32,
) -> Result<(String, Vec<SqlValue>), CompileError> {
```

Replace the WHERE-building block (the `let src_alias = alias(0);` loop and the following `for (i, t) in types.iter().enumerate()` row-filter loop, around lines 382-394) with a single per-position loop that emits eq-filters then row-filters:

```rust
    // WHERE: per position in chain order, this type's caller eq-filters then its ACL
    // row-filters, both bound at alias `t_i`. Params are pushed in conjunct-emission
    // order so positional `?` alignment holds. Source filters are just position 0's
    // eq_filters — no special case.
    let mut params = Vec::new();
    let mut conjuncts: Vec<String> = Vec::new();
    for (i, t) in types.iter().enumerate() {
        let a = alias(i);
        for (col, val) in &t.eq_filters {
            conjuncts.push(format!("({a}.{} = ?)", quote_ident(col)));
            params.push(val.clone());
        }
        for f in &t.row_filters {
            conjuncts.push(filter_sql(f, &a, &mut params));
        }
    }
```

- [ ] **Step 5: Adapt the one handler call site (behavior-preserving).**

In `src/services/query-api/src/handler.rs`, `read_linked_chain` still has `q.source_filters` (the `ChainQuery` field is reshaped in Task 2). Make these minimal edits so the lib builds and behavior is identical:

1. The initial source `ChainType` push (around line 349) gains `eq_filters: vec![]`:

```rust
    let mut ctypes: Vec<crate::sql::ChainType> = vec![crate::sql::ChainType {
        table: from_type.table.clone(),
        row_filters: s_filters,
        eq_filters: vec![],
    }];
```

2. The per-hop `ChainType` push (around line 383) gains `eq_filters: vec![]`:

```rust
        ctypes.push(crate::sql::ChainType {
            table: to_type.table.clone(),
            row_filters: t_filters,
            eq_filters: vec![],
        });
```

3. After the existing `source_filters` vec is built (the loop ending around line 409), assign it into position 0 and drop the `&source_filters` argument from the `compile_chain` call:

```rust
    // Source eq-filters bind at t_0 (position 0's eq_filters); the compiler has no
    // source special-case.
    ctypes[0].eq_filters = source_filters;

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

    let (sql, params) = compile_chain(&ctypes, &hops, &to_allowed, &to_mask_cols, DEFAULT_LIMIT)?;
```

(Keep the existing `source_filters` computation loop above this exactly as-is for Task 1; Task 2 replaces it.)

- [ ] **Step 6: Run the sql-compile unit tests — expect PASS.**

Run: `buck2 test //src/services/query-api:sql-compile > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t.log`
Expected: `Tests finished: Pass N. Fail 0.` (all chain tests, including the two new ones, pass).

- [ ] **Step 7: Regression — run the two chain e2es (behavior unchanged).**

Run one at a time (never concurrent buck2):
`buck2 test //src/services/query-api:link-traversal > /tmp/t1.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t1.log`
then
`buck2 test //src/services/query-api:multi-hop-traversal-e2e > /tmp/t2.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t2.log`
Expected: both `Fail 0` — Task 1 is behavior-preserving for existing callers.

- [ ] **Step 8: Commit.**

```bash
git add src/services/query-api/src/sql.rs src/services/query-api/src/handler.rs src/services/query-api/tests/sql_compile.rs
git commit -m "feat(query-api): per-position caller eq-filters in compile_chain

ChainType gains eq_filters bound at its alias t_i; compile_chain drops the
source-only param (source is position 0's eq_filters). Behavior-preserving for
the existing source-filter handler call site.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 2: Handler + wire — positioned, governed caller filters

Reshape the query types to carry positioned filters, govern each caller filter against its own type (visibility-then-coerce), and add the pure HTTP-side resolver that maps `<linkname>.col` keys to positions (with the relational/graph boundary). No new traversal behavior is asserted here beyond the resolver unit tests; the existing e2es (now updated to the new construction shape) are the regression net.

**Files:**
- Modify: `src/services/query-api/src/handler.rs`
- Create: `src/services/query-api/src/chain_filter.rs`
- Modify: `src/services/query-api/src/lib.rs`
- Modify: `src/services/query-api/src/http.rs`
- Create + Test: `src/services/query-api/tests/chain_filter_resolve.rs`
- Modify: `src/services/query-api/BUCK`
- Modify (compile fixes): `src/services/query-api/tests/link_traversal.rs`, `src/services/query-api/tests/multi_hop_traversal_e2e.rs`

- [ ] **Step 1: Write the resolver unit tests (failing).**

Create `src/services/query-api/tests/chain_filter_resolve.rs`:

```rust
use query_api::chain_filter::{FilterResolveError, resolve_chain_filters};
use query_api::handler::ChainFilter;

fn p(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

fn sorted(mut fs: Vec<ChainFilter>) -> Vec<(usize, String, String)> {
    let mut out: Vec<(usize, String, String)> =
        fs.drain(..).map(|f| (f.position, f.column, f.raw)).collect();
    out.sort();
    out
}

#[test]
fn bare_key_is_a_source_filter() {
    let got = resolve_chain_filters(&["placed".into()], p(&[("active", "true")])).unwrap();
    assert_eq!(sorted(got), vec![(0, "active".into(), "true".into())]);
}

#[test]
fn prefixed_key_maps_to_the_links_position() {
    let path = vec!["placed".to_string(), "contains".to_string()];
    let got = resolve_chain_filters(
        &path,
        p(&[("active", "true"), ("placed.status", "open"), ("contains.sku", "ABC")]),
    )
    .unwrap();
    assert_eq!(
        sorted(got),
        vec![
            (0, "active".into(), "true".into()),
            (1, "status".into(), "open".into()),
            (2, "sku".into(), "ABC".into()),
        ]
    );
}

#[test]
fn unknown_prefix_is_an_error() {
    let err = resolve_chain_filters(&["placed".into()], p(&[("nope.x", "1")])).unwrap_err();
    assert_eq!(err, FilterResolveError::UnknownTarget("nope".into()));
}

#[test]
fn repeated_link_prefix_is_rejected() {
    let path = vec!["knows".to_string(), "knows".to_string()];
    let err = resolve_chain_filters(&path, p(&[("knows.name", "X")])).unwrap_err();
    assert_eq!(err, FilterResolveError::AmbiguousLink("knows".into()));
}

#[test]
fn repeated_link_without_a_filter_on_it_is_fine() {
    // Plain self-traversal still resolves (no per-hop filter on the repeated link).
    let path = vec!["knows".to_string(), "knows".to_string()];
    let got = resolve_chain_filters(&path, p(&[("active", "true")])).unwrap();
    assert_eq!(sorted(got), vec![(0, "active".into(), "true".into())]);
}

#[test]
fn column_with_first_dot_split_keeps_remainder() {
    // Split on the FIRST dot only; the remainder is the column.
    let path = vec!["placed".to_string()];
    let got = resolve_chain_filters(&path, p(&[("placed.a", "1")])).unwrap();
    assert_eq!(sorted(got), vec![(1, "a".into(), "1".into())]);
}
```

- [ ] **Step 2: Add the `chain-filter-resolve` test target to BUCK.**

In `src/services/query-api/BUCK`, add (next to the other pure `rust_test`s like `filter-coerce`):

```python
rust_test(
    name = "chain-filter-resolve",
    crate = "chain_filter_resolve",
    srcs = ["tests/chain_filter_resolve.rs"],
    crate_root = "tests/chain_filter_resolve.rs",
    edition = "2024",
    deps = [":query-api"],
)
```

- [ ] **Step 3: Run the resolver test target — expect compile failure.**

Run: `buck2 test //src/services/query-api:chain-filter-resolve > /tmp/t.log 2>&1; grep -E "error\[|cannot find|unresolved|Tests finished|FAIL" /tmp/t.log`
Expected: unresolved import errors — `chain_filter` module and `ChainFilter` don't exist yet.

- [ ] **Step 4: Add the `ChainFilter` type and reshape the query structs.**

In `src/services/query-api/src/handler.rs`:

Add the `ChainFilter` struct (place it just above `ChainQuery`, around line 305):

```rust
/// A caller equality filter addressed at a chain position. `position` 0 is the source;
/// `position` k is the final target. Built by the HTTP resolver from a `<linkname>.col`
/// (or bare = source) query key; coerced + visibility-checked against the type at that
/// position in `read_linked_chain`.
#[derive(Debug, Clone)]
pub struct ChainFilter {
    pub position: usize,
    pub column: String,
    pub raw: String,
}
```

Change `ChainQuery` (around line 308) to carry positioned filters:

```rust
/// A governed multi-hop traversal: from source objects matching the position-0 filters,
/// follow `path` (an ordered list of link names), return the deduped final-target
/// objects. Every type in the chain is governed (Read + row-filters) and caller-filterable.
pub struct ChainQuery {
    pub from_type: String,
    pub path: Vec<String>,
    pub filters: Vec<ChainFilter>,
}
```

Change `LinkQuery` (around line 278) to carry positioned filters too, and update the `read_linked_objects` wrapper to forward them:

```rust
/// A governed single-hop traversal (the `N=1` chain): from source objects, follow
/// `link`, return the linked targets. `filters` are positioned (0 = source, 1 = target).
pub struct LinkQuery {
    pub from_type: String,
    pub link: String,
    pub filters: Vec<ChainFilter>,
}

pub async fn read_linked_objects(
    q: &LinkQuery,
    subject: &Subject,
    deps: &QueryDeps<'_>,
) -> Result<ObjectRows, QueryError> {
    read_linked_chain(
        &ChainQuery {
            from_type: q.from_type.clone(),
            path: vec![q.link.clone()],
            filters: q.filters.clone(),
        },
        subject,
        deps,
    )
    .await
}
```

- [ ] **Step 5: Rework `read_linked_chain` to govern per-position caller filters.**

In `src/services/query-api/src/handler.rs`, add a small module-level helper struct just above `read_linked_chain` (around line 304, after `MAX_CHAIN_DEPTH`):

```rust
/// Per-position governance metadata for a resolved chain, aligned with the `ChainType`
/// vector passed to `compile_chain` (index 0 = source, index k = final target).
struct HopMeta {
    otype: ObjectType,
    denied: std::collections::HashSet<String>,
    masked: std::collections::HashSet<String>,
}
```

Replace the body of `read_linked_chain` from the source-policy load through the end of the function (i.e. everything from `let (s_filters, s_denied, s_masked) = ...` onward) with the positioned version below. It builds `metas` alongside `ctypes`, applies each caller filter against its position (visibility-then-coerce), and derives the final-target projection from `metas.last()`:

```rust
    let (s_filters, s_denied, s_masked) = load_policy(deps.acl, &subject.0, &from_target).await?;

    // Per-position governance metadata, aligned with `ctypes` (index 0 = source).
    let mut metas: Vec<HopMeta> = vec![HopMeta {
        otype: from_type.clone(),
        denied: s_denied,
        masked: s_masked,
    }];
    let mut ctypes: Vec<crate::sql::ChainType> = vec![crate::sql::ChainType {
        table: from_type.table.clone(),
        row_filters: s_filters,
        eq_filters: vec![],
    }];
    let mut hops: Vec<control_plane_core::LinkBacking> = Vec::with_capacity(q.path.len());

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
        // Read on every hop type (the leak-free guarantee).
        if deps.acl.check(&subject.0, Action::Read, &to_target).await? == Decision::Deny {
            return Err(QueryError::Forbidden);
        }
        // A link pointing at a missing type is an internal inconsistency, not a 404.
        let to_type = deps.ontology.get_type(&to_name).await?;
        let (t_filters, t_denied, t_masked) = load_policy(deps.acl, &subject.0, &to_target).await?;
        hops.push(link.backing.clone());
        ctypes.push(crate::sql::ChainType {
            table: to_type.table.clone(),
            row_filters: t_filters,
            eq_filters: vec![],
        });
        metas.push(HopMeta {
            otype: to_type,
            denied: t_denied,
            masked: t_masked,
        });
        current_name = to_name;
    }

    // Caller filters, governed per position: visibility first (denied/masked or unknown
    // column -> 400, no type-info leak), then coerce the raw value to that position's
    // declared logical type. The coerced value is bound at the position's alias `t_i`.
    for f in &q.filters {
        if f.position >= ctypes.len() {
            return Err(QueryError::BadFilter(f.column.clone()));
        }
        let meta = &metas[f.position];
        let allowed = project_allowed(&meta.otype.properties, &meta.denied);
        if !allowed.contains(&f.column) || meta.masked.contains(&f.column) {
            return Err(QueryError::BadFilter(f.column.clone()));
        }
        let ty = meta
            .otype
            .properties
            .iter()
            .find(|p| p.name == f.column)
            .map(|p| p.ty.as_str())
            .unwrap_or("");
        let v = crate::filter::coerce_filter(&f.column, ty, &f.raw)
            .map_err(|_| QueryError::BadFilter(f.column.clone()))?;
        ctypes[f.position].eq_filters.push((f.column.clone(), v));
    }

    // Final-target projection, from the last position (path is non-empty => >= 2 metas).
    let target = metas.last().expect("non-empty path yields a final target");
    let to_allowed = project_allowed(&target.otype.properties, &target.denied);
    if to_allowed.is_empty() {
        return Err(QueryError::Forbidden);
    }
    let to_mask_cols: Vec<String> = to_allowed
        .iter()
        .filter(|c| target.masked.contains(*c))
        .cloned()
        .collect();

    let (sql, params) = compile_chain(&ctypes, &hops, &to_allowed, &to_mask_cols, DEFAULT_LIMIT)?;
    let served = deps.serving.fetch_rows(&sql, &params).await?;
    let logical_types: Vec<String> = to_allowed
        .iter()
        .map(|name| {
            target
                .otype
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

This removes the old `target_type`/`target_denied`/`target_masked` accumulators, the separate `from_allowed`/`source_filters` block, and the `ctypes[0].eq_filters = source_filters;` line added in Task 1 (all superseded by the positioned loop). Ensure no now-unused variables remain (the previous `from_allowed`/`s_masked`-as-`source_filters` code is gone).

- [ ] **Step 6: Create the resolver module.**

Create `src/services/query-api/src/chain_filter.rs`:

```rust
//! Pure resolution of HTTP query-param filter keys into positioned chain filters.
//! A bare key (`col`) is a source filter (position 0); a `<linkname>.col` key targets
//! the type that link reaches in `path`. This is the relational `/links` surface: a link
//! that repeats in the path makes a per-hop filter on it ambiguous, which is the boundary
//! of the deferred graph (`/graph`) capability — such a filter is rejected here.

use crate::handler::ChainFilter;

#[derive(Debug, thiserror::Error, PartialEq)]
pub enum FilterResolveError {
    /// A `<prefix>.col` key whose `prefix` names no link in the path.
    #[error("unknown filter target: {0}")]
    UnknownTarget(String),
    /// A per-hop filter on a link that repeats in the path. Per-hop filtering across a
    /// repeated link is graph traversal (deferred to /graph), not relational traversal.
    #[error("ambiguous filter link '{0}': graph traversal (/graph) is not yet supported")]
    AmbiguousLink(String),
}

/// Resolve `(key, value)` params against `path` into positioned `ChainFilter`s. A key
/// containing a `.` is `<prefix>.<column>` (split on the FIRST dot); `prefix` must name a
/// link occurring exactly once in `path` (→ that link's position = index + 1). A key with
/// no `.` is a source filter (position 0).
pub fn resolve_chain_filters(
    path: &[String],
    params: Vec<(String, String)>,
) -> Result<Vec<ChainFilter>, FilterResolveError> {
    let mut counts: std::collections::HashMap<&str, usize> = std::collections::HashMap::new();
    for l in path {
        *counts.entry(l.as_str()).or_default() += 1;
    }
    let mut out = Vec::with_capacity(params.len());
    for (key, value) in params {
        match key.split_once('.') {
            None => out.push(ChainFilter {
                position: 0,
                column: key,
                raw: value,
            }),
            Some((prefix, column)) => match counts.get(prefix).copied().unwrap_or(0) {
                0 => return Err(FilterResolveError::UnknownTarget(prefix.to_string())),
                1 => {
                    let idx = path
                        .iter()
                        .position(|l| l == prefix)
                        .expect("count == 1 implies present");
                    out.push(ChainFilter {
                        position: idx + 1,
                        column: column.to_string(),
                        raw: value,
                    });
                }
                _ => return Err(FilterResolveError::AmbiguousLink(prefix.to_string())),
            },
        }
    }
    Ok(out)
}
```

- [ ] **Step 7: Register the module.**

In `src/services/query-api/src/lib.rs`, add `pub mod chain_filter;` alongside the existing `pub mod filter;` (keep the module list alphabetical if it already is).

- [ ] **Step 8: Run the resolver unit tests — expect PASS.**

Run: `buck2 test //src/services/query-api:chain-filter-resolve > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t.log`
Expected: `Tests finished: Pass 6. Fail 0.`

- [ ] **Step 9: Wire the resolver into the HTTP layer.**

In `src/services/query-api/src/http.rs`:

In `get_linked` (single-hop), replace the `source_filters` collection + `LinkQuery` construction. The handler now resolves `<link>.col` → position 1 and bare → position 0:

```rust
    // Resolve filter keys against the single-link path: bare -> source (t_0), `<link>.col`
    // -> target (t_1). A bad prefix -> 400.
    let params: Vec<(String, String)> = params.into_iter().collect();
    let filters = match crate::chain_filter::resolve_chain_filters(
        std::slice::from_ref(&link_name),
        params,
    ) {
        Ok(f) => f,
        Err(e) => return (StatusCode::BAD_REQUEST, e.to_string()).into_response(),
    };
    let deps = QueryDeps {
        ontology: st.cp.ontology(),
        acl: st.cp.acl(),
        serving: st.serving.as_ref(),
    };
    match read_linked_objects(
        &LinkQuery {
            from_type,
            link: link_name,
            filters,
        },
        &Subject(SubjectId(subject)),
        &deps,
    )
    .await
```

In `get_linked_chain`, after parsing `path` (keep the existing `params.remove("path")` block), replace the `source_filters` collection + `ChainQuery` construction:

```rust
    // Remaining params are filters: bare -> source, `<linkname>.col` -> that link's
    // position. Unknown/ambiguous prefix -> 400 (ambiguous = the relational/graph boundary).
    let params: Vec<(String, String)> = params.into_iter().collect();
    let filters = match crate::chain_filter::resolve_chain_filters(&path, params) {
        Ok(f) => f,
        Err(e) => return (StatusCode::BAD_REQUEST, e.to_string()).into_response(),
    };
    let deps = QueryDeps {
        ontology: st.cp.ontology(),
        acl: st.cp.acl(),
        serving: st.serving.as_ref(),
    };
    match read_linked_chain(
        &ChainQuery {
            from_type,
            path,
            filters,
        },
        &Subject(SubjectId(subject)),
        &deps,
    )
    .await
```

(The `match` arms mapping `QueryError` → status codes are unchanged.)

- [ ] **Step 10: Update the e2e construction sites to the new filter shape.**

In `src/services/query-api/tests/link_traversal.rs`: add `ChainFilter` to the `query_api::handler` import line and a small helper near the top (after the `tref` helper):

```rust
use query_api::handler::{
    ChainFilter, LinkQuery, QueryDeps, QueryError, Subject, read_linked_objects,
};
```

```rust
fn srcf(col: &str, val: &str) -> ChainFilter {
    ChainFilter {
        position: 0,
        column: col.into(),
        raw: val.into(),
    }
}
```

Then in every `LinkQuery { ... }` literal, replace the `source_filters: ...` field:
- `source_filters: vec![("region".into(), "CA".into())]` → `filters: vec![srcf("region", "CA")]`
- `source_filters: vec![]` → `filters: vec![]`

(There are several: `fk_traversal_returns_linked_targets`, `missing_read_on_source_is_forbidden`, `missing_read_on_target_is_forbidden`, `source_row_filter_closes_the_leak`, `target_row_filter_and_projection_apply`, `source_filter_on_denied_column_is_bad_filter`, `many_to_many_dedups_shared_targets`, `unknown_link_is_reported`.)

In `src/services/query-api/tests/multi_hop_traversal_e2e.rs`: add `ChainFilter` to the `query_api::handler` import and the same `srcf` helper (after the `tref` helper):

```rust
use query_api::handler::{ChainFilter, ChainQuery, QueryDeps, QueryError, Subject, read_linked_chain};
```

```rust
fn srcf(col: &str, val: &str) -> ChainFilter {
    ChainFilter {
        position: 0,
        column: col.into(),
        raw: val.into(),
    }
}
```

Then in the three `ChainQuery { ... }` literals, replace the field:
- `source_filters: vec![("region".into(), "CA".into())]` → `filters: vec![srcf("region", "CA")]`
- `source_filters: vec![]` → `filters: vec![]`

- [ ] **Step 11: Build the lib and run the affected targets one at a time — expect PASS.**

Run sequentially (never two buck2 at once):
`buck2 test //src/services/query-api:sql-compile > /tmp/t0.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t0.log`
`buck2 test //src/services/query-api:chain-filter-resolve > /tmp/t1.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t1.log`
`buck2 test //src/services/query-api:link-traversal > /tmp/t2.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t2.log`
`buck2 test //src/services/query-api:multi-hop-traversal-e2e > /tmp/t3.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t3.log`
Expected: all `Fail 0`. (The existing traversal behavior is unchanged; only the construction shape changed.)

- [ ] **Step 12: Clippy on the crate — expect clean.**

Run: `buck2 build '//src/services/query-api:query-api[clippy.txt]' > /tmp/c.log 2>&1; cat $(buck2 build --show-full-output '//src/services/query-api:query-api[clippy.txt]' 2>/dev/null | awk '{print $2}')` — or simply `./tools/clippy-all.sh > /tmp/c.log 2>&1; grep -iE "warning|error" /tmp/c.log || echo CLEAN`.
Expected: no warnings for the query-api crate (in particular, no `dead_code`/`unused` from the reshape).

- [ ] **Step 13: Commit.**

```bash
git add src/services/query-api/src/handler.rs src/services/query-api/src/chain_filter.rs src/services/query-api/src/lib.rs src/services/query-api/src/http.rs src/services/query-api/BUCK src/services/query-api/tests/chain_filter_resolve.rs src/services/query-api/tests/link_traversal.rs src/services/query-api/tests/multi_hop_traversal_e2e.rs
git commit -m "feat(query-api): positioned governed caller filters on traversal chains

ChainFilter addresses a caller eq-filter at a chain position; read_linked_chain
governs each per-type (visibility-then-coerce) and binds it at t_i. A pure
resolver maps <linkname>.col keys to positions (bare = source); a filter on a
repeated link is rejected as the relational/graph boundary.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 3: e2e — positioned filters narrow, coerce, and stay governed

Prove end-to-end (through DuckDB) that caller filters on the final target and an intermediate type narrow the result, that a non-text intermediate column coerces (where a text bind would match nothing), and that governance still holds (denied target column → 400; out-of-range position → 400).

**Files:**
- Test: `src/services/query-api/tests/multi_hop_traversal_e2e.rs` (extend; reuses the existing `setup`, `subject_with_role`, `grant_read` helpers and the `Customer → Order → LineItem` fixture)

- [ ] **Step 1: Add a positioned-filter helper and the new tests (failing).**

In `src/services/query-api/tests/multi_hop_traversal_e2e.rs`, add a helper for non-source positions near `srcf` (added in Task 2):

```rust
fn hopf(position: usize, col: &str, val: &str) -> ChainFilter {
    ChainFilter {
        position,
        column: col.into(),
        raw: val.into(),
    }
}
```

Append these tests. They reuse `setup` (Customer 1=CA → orders 10 shipped, 11 pending → line_items 100,101 (order 10), 102 (order 11); Customer 2=NY → order 20 → line_item 200):

```rust
#[tokio::test(flavor = "multi_thread")]
async fn target_filter_narrows_final_set() {
    let fx = PgFixture::start();
    let (cp, eng, _writer) = setup(&fx).await;
    let deps = QueryDeps {
        ontology: &cp,
        acl: &cp,
        serving: &eng,
    };
    let (a, role) = subject_with_role(&cp, "alice").await;
    grant_read(&cp, &role, "Customer").await;
    grant_read(&cp, &role, "Order").await;
    grant_read(&cp, &role, "LineItem").await;

    // Customer 1 (CA) reaches line_items 100,101,102; a final-target sku filter narrows.
    let rows = read_linked_chain(
        &ChainQuery {
            from_type: "Customer".into(),
            path: vec!["orders".into(), "lineItems".into()],
            filters: vec![srcf("region", "CA"), hopf(2, "sku", "A")],
        },
        &Subject(a),
        &deps,
    )
    .await
    .unwrap();
    let body = objects_to_json(&rows);
    let ids: Vec<String> = body["objects"]
        .as_array()
        .unwrap()
        .iter()
        .map(|o| o["id"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(ids, vec!["100".to_string()], "only line_item 100 has sku=A");
}

#[tokio::test(flavor = "multi_thread")]
async fn intermediate_typed_filter_coerces_and_narrows() {
    let fx = PgFixture::start();
    let (cp, eng, _writer) = setup(&fx).await;
    let deps = QueryDeps {
        ontology: &cp,
        acl: &cp,
        serving: &eng,
    };
    let (a, role) = subject_with_role(&cp, "alice").await;
    grant_read(&cp, &role, "Customer").await;
    grant_read(&cp, &role, "Order").await;
    grant_read(&cp, &role, "LineItem").await;

    // Intermediate Order.id is Long: filtering id=10 must coerce to Int(10). A text bind
    // ("10" against a BIGINT column) would match nothing; typed coercion matches order 10
    // -> line_items 100,101 (102 hangs off order 11, excluded).
    let rows = read_linked_chain(
        &ChainQuery {
            from_type: "Customer".into(),
            path: vec!["orders".into(), "lineItems".into()],
            filters: vec![srcf("region", "CA"), hopf(1, "id", "10")],
        },
        &Subject(a),
        &deps,
    )
    .await
    .unwrap();
    let body = objects_to_json(&rows);
    let mut ids: Vec<String> = body["objects"]
        .as_array()
        .unwrap()
        .iter()
        .map(|o| o["id"].as_str().unwrap().to_string())
        .collect();
    ids.sort();
    assert_eq!(ids, vec!["100".to_string(), "101".to_string()]);
}

#[tokio::test(flavor = "multi_thread")]
async fn source_and_intermediate_filters_combine() {
    let fx = PgFixture::start();
    let (cp, eng, _writer) = setup(&fx).await;
    let deps = QueryDeps {
        ontology: &cp,
        acl: &cp,
        serving: &eng,
    };
    let (a, role) = subject_with_role(&cp, "alice").await;
    grant_read(&cp, &role, "Customer").await;
    grant_read(&cp, &role, "Order").await;
    grant_read(&cp, &role, "LineItem").await;

    // Source region=CA AND intermediate Order.status=pending -> only order 11 -> line_item 102.
    let rows = read_linked_chain(
        &ChainQuery {
            from_type: "Customer".into(),
            path: vec!["orders".into(), "lineItems".into()],
            filters: vec![srcf("region", "CA"), hopf(1, "status", "pending")],
        },
        &Subject(a),
        &deps,
    )
    .await
    .unwrap();
    let body = objects_to_json(&rows);
    let ids: Vec<String> = body["objects"]
        .as_array()
        .unwrap()
        .iter()
        .map(|o| o["id"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(ids, vec!["102".to_string()]);
}

#[tokio::test(flavor = "multi_thread")]
async fn bad_positioned_filters_are_rejected() {
    let fx = PgFixture::start();
    let (cp, eng, _writer) = setup(&fx).await;
    let deps = QueryDeps {
        ontology: &cp,
        acl: &cp,
        serving: &eng,
    };
    let (a, role) = subject_with_role(&cp, "alice").await;
    grant_read(&cp, &role, "Customer").await;
    grant_read(&cp, &role, "Order").await;
    grant_read(&cp, &role, "LineItem").await;
    // Deny the final-target sku column for this subject.
    cp.set_policy(
        &role,
        Policy {
            target: PolicyTarget::Type(TypeName("LineItem".into())),
            row_filter: None,
            deny_columns: vec!["sku".into()],
            mask_columns: vec![],
        },
    )
    .await
    .unwrap();

    // A filter on a denied target column -> BadFilter (visibility before coercion).
    let denied = read_linked_chain(
        &ChainQuery {
            from_type: "Customer".into(),
            path: vec!["orders".into(), "lineItems".into()],
            filters: vec![hopf(2, "sku", "A")],
        },
        &Subject(a.clone()),
        &deps,
    )
    .await
    .unwrap_err();
    assert!(
        matches!(denied, QueryError::BadFilter(c) if c == "sku"),
        "denied target column filter -> BadFilter; got {denied:?}"
    );

    // A position past the end of the chain -> BadFilter (guarded, never panics).
    let oob = read_linked_chain(
        &ChainQuery {
            from_type: "Customer".into(),
            path: vec!["orders".into(), "lineItems".into()],
            filters: vec![hopf(5, "id", "1")],
        },
        &Subject(a),
        &deps,
    )
    .await
    .unwrap_err();
    assert!(
        matches!(oob, QueryError::BadFilter(c) if c == "id"),
        "out-of-range position -> BadFilter; got {oob:?}"
    );
}
```

- [ ] **Step 2: Run the e2e target — expect PASS.**

Run: `buck2 test //src/services/query-api:multi-hop-traversal-e2e > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t.log`
Expected: `Tests finished: Pass 5. Fail 0.` (the original `multi_hop_served_and_governed` plus the four new tests).

- [ ] **Step 3: Commit.**

```bash
git add src/services/query-api/tests/multi_hop_traversal_e2e.rs
git commit -m "test(query-api): e2e for positioned target/intermediate filters

Final-target and intermediate filters narrow the result; an intermediate Long
column coerces (a text bind would match nothing); denied target column and
out-of-range position both reject as BadFilter.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 4: Docs — roadmap delivered marker + FUTURE.md defer items

**Files:**
- Modify: `docs/superpowers/specs/2026-06-06-loom-roadmap.md`
- Modify: `docs/FUTURE.md`

- [ ] **Step 1: Mark the slice delivered in the roadmap.**

In `docs/superpowers/specs/2026-06-06-loom-roadmap.md`, under the **Query / read path** bullets (after the multi-hop traversal part-5 entry), add a delivered entry:

```markdown
  - *Part 6 — target / intermediate filters (slice C part-2)* ✅ DELIVERED
    (`2026-06-16-query-target-intermediate-filters-design.md`). A traversal caller can now
    filter **any** type in a chain (source, every intermediate, final target) by typed equality,
    addressed by a `<linkname>.<column>` query key (bare = source). Each per-hop filter is
    coerced to its own type's logical type (via `filter::coerce_filter`) and visibility-checked
    against its own type's governed projection; the N-ends Read governance is unchanged, so
    caller filters only narrow within already-permitted visibility. Draws the relational
    (`/links`) vs deferred graph (`/graph`) boundary: a per-hop filter on a link that repeats in
    the path is rejected (the graph case). Proven by compiler unit tests, a pure resolver unit,
    and a chain e2e.
```

In the **"Where we are"** section, update the typed-input-filters paragraph's closing so the "remaining smaller query follow-ups" no longer implies target filtering is pending — append after the typed-input-filters sentence:

```markdown
**Target / intermediate filters** (slice C part-2,
`2026-06-16-query-target-intermediate-filters-design.md`) are now delivered too: every type a
traversal touches is caller-filterable (typed, governed per type), not just the source, and the
relational-vs-graph boundary is drawn (a future `/graph` surface owns cyclic/self-link
traversal). The remaining slice-C parts are inverse-direction hops, object-set inputs, and the
source→target association.
```

- [ ] **Step 2: Add the FUTURE.md defer items.**

In `docs/FUTURE.md`, under the **"Ontology & read path (Step 3)"** section, replace the "Caller-supplied target / intermediate filters" bullet in the multi-hop block (it is now delivered) — change that bullet to a delivered note and add the new graph-traversal item. Find the bullet beginning "**Caller-supplied target / intermediate filters.**" and replace it with:

```markdown
- **Caller-supplied target / intermediate filters.** ✅ DELIVERED
  (`2026-06-16-query-target-intermediate-filters-design.md`): per-hop typed equality filters on
  any type in a chain, addressed by `<linkname>.<column>` (bare = source), governed per type.
```

Then add a new block at the end of the "Ontology & read path (Step 3)" section:

```markdown
From the target/intermediate-filters slice (`2026-06-16-query-target-intermediate-filters-design.md`),
which made every type in a traversal chain caller-filterable (typed, governed per position) and
drew the relational/graph boundary:

- **Graph traversal as a first-class concept.** The relational `/links` chain is a fixed, acyclic
  set of INNER JOINs over DuckLake tables — distinct link names per path, so link-name filter
  addressing is unambiguous. Genuine graph traversal (self-links, cycles, friend-of-friend,
  hierarchies, variable-length / recursive paths) belongs to a separate, deferred `/graph` surface
  (a tree is a special case; a `/tree` surface can split out later if it earns its keep — not
  committed now), with its own execution (recursive CTEs, cycle guards) and graph-aware filter
  addressing (positional or per-occurrence) that resolves the repeated-link case `/links` rejects.
  Plain self-traversal still *works* on `/links` (no regression) — only a per-hop *filter* on a
  repeated link is refused.
- **Comparison / set operators on per-hop filters.** Equality-only here, inheriting the
  typed-input-filters comparison-operators follow-up; a shared richer filter grammar would cover
  source and per-hop filters at once.
```

- [ ] **Step 3: Run the markdown lint hooks and commit whatever they fix.**

Run: `buck2 run //tools:prek -- run --all-files > /tmp/lint.log 2>&1; grep -iE "Failed|Passed|fixing|reformatted" /tmp/lint.log`
Expected: the `end-of-file-fixer` / `trim trailing whitespace` hooks pass (or fix-in-place; if they changed files, the edits are already applied). Ensure both `.md` files end with exactly one trailing newline.

```bash
git add docs/superpowers/specs/2026-06-06-loom-roadmap.md docs/FUTURE.md
git commit -m "docs(query): target/intermediate filters delivered; defer graph traversal

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

- [ ] **Step 4: Full query-api test sweep (final regression).**

Run: `buck2 test //src/services/query-api/... > /tmp/sweep.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/sweep.log`
Expected: `Fail 0` across the whole query-api suite.

---

## Self-Review

**Spec coverage:**
- "Filter any type in the chain, addressed by `<linkname>.col`, bare = source" → Task 2 (`ChainFilter`, resolver) + Task 1 (compiler binds per position). ✓
- "Per-type typed coercion + per-type visibility check" → Task 2 Step 5 (visibility-then-coerce against `metas[position]`). ✓
- "Single-hop inherits target filtering for free" → Task 2 Step 9 (`get_linked` resolves against `[link]`). ✓
- "Relational/graph boundary: repeated-link per-hop filter rejected, plain traversal not regressed" → Task 2 (resolver `AmbiguousLink`) + resolver unit tests (`repeated_link_prefix_is_rejected`, `repeated_link_without_a_filter_on_it_is_fine`). ✓
- "Errors reuse `BadFilter`/`BadChain`, no widening" → Task 2 uses `BadFilter` for column/coercion/out-of-range; resolver errors map to 400 in http.rs; no new `QueryError` variant added. ✓
- "compile_chain folds source into types[0].eq_filters" → Task 1. ✓
- Testing (unit compiler, resolver unit, e2e narrowing+typed+governance, regression) → Tasks 1/2/3. ✓
- Docs (roadmap delivered, FUTURE.md graph defer) → Task 4. ✓

**Placeholder scan:** No TBD/TODO; every code step shows full code and exact expected output. ✓

**Type consistency:** `ChainFilter { position: usize, column: String, raw: String }` is defined once (Task 2 Step 4) and constructed identically by `srcf`/`hopf` and the resolver. `ChainType` gains `eq_filters: Vec<(String, SqlValue)>` (Task 1) and every literal sets it. `compile_chain(types, hops, allowed_cols, mask_cols, limit)` — 5 args — is called consistently in tests and handler. `resolve_chain_filters(&[String], Vec<(String,String)>) -> Result<Vec<ChainFilter>, FilterResolveError>` matches its call sites. ✓

**Note on a transitional line:** Task 1 Step 5 adds `ctypes[0].eq_filters = source_filters;`; Task 2 Step 5 removes it as part of replacing the whole tail of `read_linked_chain`. This is intentional (keeps each commit building) and called out in Task 2 Step 5.
