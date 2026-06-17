# Object Identity + Source→Target Association Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a first-class object identity (primary key) to the ontology, then a governed traversal read that returns source↔target identity pairs (the edge list) instead of the DISTINCT-collapsed target set.

**Architecture:** `ObjectType` gains an optional `identity` naming its PK property; it persists in `ontology.object_type` and is validated at bind. A new `read_associations` resolves a chain exactly like `read_linked_chain` (shared `resolve_chain` helper), then projects the two end-position identity columns via a new `compile_chain_pairs` (shared FROM/WHERE builder with `compile_chain_with`). A `?shape=association` flag on the existing chain routes selects it.

**Tech Stack:** Rust, buck2, sqlx compile-time macros (postgres adapter), DuckLake catalog over Postgres, DuckDB serving engine, hermetic Postgres/DuckDB fixture tests.

**Design:** `docs/superpowers/specs/2026-06-17-object-identity-association-design.md`

## Global Constraints

- Never run two `buck2` commands concurrently. One at a time.
- Never pipe `buck2 test`/`bxl` through `tail`/`head` — redirect and grep:
  `buck2 test //target > /tmp/t.log 2>&1; grep -nE "Tests finished|Pass [0-9]|FAIL|error\[|panicked" /tmp/t.log`. Fixture tests boot hermetic Postgres+DuckDB and take minutes — allow up to 600000ms.
- Tests are integration `rust_test`/`loom_fixture_test` targets only — never inline `#[test]` in `src/**` (the `no-inline-tests` hook fails the build).
- After changing any postgres SQL (the `query!`/`query_scalar!` macros), run `./tools/sqlx-prepare.sh` and commit the `.sqlx/` change.
- If the rustfmt pre-commit hook fails, run `buck2 run //tools:rustfmt -- <files>`, re-stage, re-commit (whitespace only).
- Property names equal physical column names (the bind path validates `property.name` against `schema.columns` by name), so an identity property name is used directly as a SQL column identifier.
- Commit trailer: `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>`

---

## Task 1: Object identity — core field + storage + round-trip contract

**Files:**
- Modify: `src/control-plane/core/src/ontology.rs` (add field to `ObjectType`)
- Create: `src/control-plane/postgres/migrations/0014_object_identity.sql`
- Modify: `src/control-plane/postgres/src/ontology.rs` (define_type upsert + get_type select)
- Modify: ~32 files holding `ObjectType { … }` literals (mechanical `identity: None`)
- Modify: `src/control-plane/testkit/src/lib.rs` (`ontology_contract` round-trip assertions)
- Refresh: `src/control-plane/postgres/.sqlx/`

**Interfaces:**
- Produces: `ObjectType.identity: Option<String>` — names the PK property; `None` = none declared. Persisted and returned by `define_type`/`get_type` on both adapters.

- [ ] **Step 1: Write the migration**

Create `src/control-plane/postgres/migrations/0014_object_identity.sql`:

```sql
-- The property that is a type's primary key, if declared. Names a row in
-- ontology.property for the same type_name. Nullable: identity is opt-in.
alter table ontology.object_type add column identity text;
```

- [ ] **Step 2: Add the field to `ObjectType`**

In `src/control-plane/core/src/ontology.rs`, add the field to the `ObjectType` struct (after `pub table: TableRef,`):

```rust
    /// The property that is this type's primary key, if declared. Names one of
    /// `properties`. `None` = no declared identity (back-compatible).
    pub identity: Option<String>,
```

- [ ] **Step 3: Mechanical sweep — add `identity: None` to every existing literal**

Adding the field breaks every `ObjectType { … }` literal. Find them all and add `identity: None,`:

Run: `git grep -n "ObjectType {" -- 'src/**/*.rs' | grep -v "pub struct ObjectType"`

For EACH match (≈32 sites across `testkit/src/lib.rs`, `postgres/src/ontology.rs`, and the `tests/*.rs` files listed by the grep), add `identity: None,` as a field in the struct literal — EXCEPT the one in `src/control-plane/postgres/src/ontology.rs` `get_type` (handled in Step 5, which sets the real value). The memory adapter needs no change (it stores the whole struct).

After editing, build to confirm the field is wired everywhere (this will still fail on the postgres SQL until Step 5/6, so build only the core + memory + the test crates that don't touch postgres SQL):

Run: `buck2 build //src/control-plane/core:core //src/control-plane/memory:memory > /tmp/b.log 2>&1; grep -nE "BUILD SUCCEEDED|error\[" /tmp/b.log`
Expected: BUILD SUCCEEDED.

- [ ] **Step 4: Add the round-trip assertions to the ontology contract**

In `src/control-plane/testkit/src/lib.rs`, inside `ontology_contract` (starts ~line 513), after an existing `define_type`/`get_type` round-trip, add a type that declares an identity and assert it round-trips. Find the `let customer = ObjectType {` literal and give it `identity: Some("id".into())` (assuming it has an `id` property — if its first property is named differently, use that name), then after its `define_type`, assert:

```rust
    // identity round-trips (declared PK property name persists).
    let got_customer = o.get_type(&tn("Customer")).await.unwrap();
    assert_eq!(
        got_customer.identity.as_deref(),
        Some("id"),
        "declared identity persists through define_type/get_type"
    );
```

And confirm a type with no identity stays `None` — find the `order` type literal (left `identity: None` in Step 3) and after its round-trip assert:

```rust
    assert_eq!(
        o.get_type(&tn("Order")).await.unwrap().identity,
        None,
        "an undeclared identity stays None"
    );
```

(Verify the exact type names/property names against the existing contract literals; use whatever the `customer`/`order` literals actually declare. The `id` property must exist on the customer type — if not, add a `PropertyDef { name: "id".into(), ty: "long".into(), required: true }` to it and set identity to match.)

- [ ] **Step 5: Wire identity through the postgres adapter**

In `src/control-plane/postgres/src/ontology.rs`:

`define_type` — change the `object_type` upsert to include `identity` (add the column, the `$4` bind, and the conflict update):

```rust
        sqlx::query!(
            "insert into ontology.object_type (name, table_schema, table_name, identity) \
             values ($1, $2, $3, $4) \
             on conflict (name) do update set table_schema = excluded.table_schema, \
                 table_name = excluded.table_name, identity = excluded.identity",
            ty.name.0,
            ty.table.schema,
            ty.table.name,
            ty.identity,
        )
```

`get_type` — change the first SELECT to read `identity` and set it on the returned `ObjectType`:

```rust
        let row = sqlx::query!(
            "select table_schema, table_name, identity from ontology.object_type where name = $1",
            name.0,
        )
```

and in the `Ok(ObjectType { … })` construction add:

```rust
            identity: row.identity,
```

- [ ] **Step 6: Refresh the sqlx cache**

Run: `./tools/sqlx-prepare.sh > /tmp/sqlx.log 2>&1; grep -nE "error|prepared|finished|Failed" /tmp/sqlx.log`
Expected: completes; `git status src/control-plane/postgres/.sqlx/` shows the changed object_type insert/select query JSONs.

- [ ] **Step 7: Run the ontology contract on both adapters**

Run: `buck2 test //src/control-plane/memory:ontology > /tmp/t1.log 2>&1; grep -nE "Tests finished|Pass [0-9]|FAIL|panicked" /tmp/t1.log`
Expected: PASS (memory round-trips identity for free).
Run: `buck2 test //src/control-plane/postgres:ontology-conformance > /tmp/t2.log 2>&1; grep -nE "Tests finished|Pass [0-9]|FAIL|panicked" /tmp/t2.log`
Expected: PASS (verify the exact target name with `grep -nE "name = \"ontology" src/control-plane/postgres/BUCK` if it differs).

- [ ] **Step 8: Build the whole control plane + clippy**

Run: `buck2 build //src/control-plane/... > /tmp/b.log 2>&1; grep -nE "BUILD SUCCEEDED|error\[" /tmp/b.log`
Run: `tools/clippy-all.sh > /tmp/c.log 2>&1; grep -nE "warning|error" /tmp/c.log || echo CLEAN`
Expected: BUILD SUCCEEDED; CLEAN.

- [ ] **Step 9: Commit**

```bash
git add src/control-plane/ docs/superpowers/plans/2026-06-17-object-identity-association.md
git commit -m "feat(ontology): first-class object identity (primary key) on ObjectType

ObjectType gains an optional identity naming its PK property; persisted in
ontology.object_type (new nullable column) and round-tripped through
define_type/get_type on both adapters. None = no declared identity
(back-compatible). Mechanical identity: None sweep across existing literals.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 2: Bind-time identity validation

**Files:**
- Modify: `src/services/ingest/src/bind.rs` (`BadIdentity` variant + validation)
- Modify/Create: `src/services/ingest/tests/bind.rs` (identity accept/reject matrix)

**Interfaces:**
- Consumes: `ObjectType.identity: Option<String>` (Task 1).
- Produces: `BindViolationReason::BadIdentity(String)` — raised when `identity` names no declared property, or names a non-required property.

- [ ] **Step 1: Write the failing bind tests**

In `src/services/ingest/tests/bind.rs`, add tests covering the identity rules. Mirror the existing test setup in that file (it already lands a table and calls `bind`). Add (adapt the table/landing helpers to the file's existing ones):

```rust
#[tokio::test]
async fn bind_accepts_identity_naming_a_required_property() {
    // ... existing harness: land a table with a required `id` column, build an
    // ObjectType whose `id` property is required, identity = Some("id") ...
    // assert bind(...).await is Ok(())
}

#[tokio::test]
async fn bind_rejects_identity_naming_unknown_property() {
    // identity = Some("nope") where no property "nope" exists ->
    // BindError::DoesNotConform containing BindViolationReason::BadIdentity(_)
}

#[tokio::test]
async fn bind_rejects_identity_naming_non_required_property() {
    // a property "label" with required: false, identity = Some("label") ->
    // BindViolationReason::BadIdentity(_)
}
```

(Use the file's existing fixture/landing helpers and assertion style; the key assertions are: Ok for required-property identity, and a `BadIdentity` violation for unknown / non-required identity. Match on `matches!(reason, BindViolationReason::BadIdentity(_))`.)

- [ ] **Step 2: Run the tests to verify they fail**

Run: `buck2 test //src/services/ingest:bind > /tmp/t.log 2>&1; grep -nE "Tests finished|FAIL|error\[|panicked|BadIdentity" /tmp/t.log`
Expected: FAIL to compile (`BadIdentity` doesn't exist) or the reject tests fail (bind doesn't validate identity yet). (Confirm the exact target name via `grep -nE "name = \"bind" src/services/ingest/BUCK`.)

- [ ] **Step 3: Add the `BadIdentity` variant**

In `src/services/ingest/src/bind.rs`, add to `BindViolationReason`:

```rust
    BadIdentity(String), // identity names no declared property, or a non-required one
```

- [ ] **Step 4: Add the validation**

In `bind`, after the property loop (after the `for p in &type_def.properties { … }` block, before the `if !violations.is_empty()` check), add:

```rust
    // Identity (if declared) must name a declared, required property — a primary key
    // cannot be nullable. The violation's `property` is the named identity column.
    if let Some(id) = &type_def.identity {
        match type_def.properties.iter().find(|p| &p.name == id) {
            None => violations.push(BindViolation {
                property: id.clone(),
                reason: BindViolationReason::BadIdentity("names no declared property".into()),
            }),
            Some(p) if !p.required => violations.push(BindViolation {
                property: id.clone(),
                reason: BindViolationReason::BadIdentity("names a non-required property".into()),
            }),
            Some(_) => {}
        }
    }
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `buck2 test //src/services/ingest:bind > /tmp/t.log 2>&1; grep -nE "Tests finished|Pass [0-9]|FAIL|panicked" /tmp/t.log`
Expected: PASS.

- [ ] **Step 6: Clippy + commit**

Run: `tools/clippy-all.sh > /tmp/c.log 2>&1; grep -nE "warning|error" /tmp/c.log || echo CLEAN`
Expected: CLEAN.

```bash
git add src/services/ingest/
git commit -m "feat(ingest): validate object identity at bind

A declared identity must name a property that exists and is required (a primary
key cannot be nullable). New BindViolationReason::BadIdentity, collected like
the other per-property violations; nothing persists on rejection.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 3: Association core — compiler + resolver + `read_associations`

**Files:**
- Modify: `src/services/query-api/src/sql.rs` (extract `chain_from_where`; add `compile_chain_pairs`)
- Modify: `src/services/query-api/src/handler.rs` (extract `resolve_chain`; add `Associations`, `read_associations`, `QueryError::NoIdentity`)
- Create: `src/services/query-api/tests/compile_chain_pairs.rs` (compiler unit) + BUCK target
- Create: `src/services/query-api/tests/associations.rs` (handler test on memory) + BUCK target

**Interfaces:**
- Consumes: `ChainType`, `compile_chain_with`'s join/filter machinery; `ChainQuery`, `HopMeta`, `project_allowed`, `Subject`, `QueryDeps`.
- Produces:
  - `fn compile_chain_pairs(dialect, types: &[ChainType], hops: &[LinkBacking], source_id: &str, target_id: &str, limit: u32) -> Result<(String, Vec<SqlValue>), CompileError>`
  - `struct Associations { from_id_type: String, to_id_type: String, pairs: Vec<(SqlValue, SqlValue)> }`
  - `async fn read_associations(q: &ChainQuery, subject: &Subject, deps: &QueryDeps) -> Result<Associations, QueryError>`
  - `QueryError::NoIdentity(String)`

- [ ] **Step 1: Write the compiler unit test (failing)**

Create `src/services/query-api/tests/compile_chain_pairs.rs`:

```rust
//! compile_chain_pairs projects the two end-position identity columns through the same
//! governed joins as compile_chain_with.

use control_plane_core::LinkBacking;
use query_api::sql::{ChainType, compile_chain_pairs, DuckDbDialect};

#[test]
fn pairs_project_source_and_target_identity() {
    let types = vec![
        ChainType {
            table: control_plane_core::TableRef { schema: "main".into(), name: "customer".into() },
            row_filters: vec![],
            predicates: vec![],
        },
        ChainType {
            table: control_plane_core::TableRef { schema: "main".into(), name: "order".into() },
            row_filters: vec![],
            predicates: vec![],
        },
    ];
    let hops = vec![LinkBacking::ForeignKey {
        from_column: "id".into(),
        to_column: "customer_id".into(),
    }];
    let (sql, params) = compile_chain_pairs(&DuckDbDialect, &types, &hops, "id", "order_id", 1000).unwrap();
    assert!(params.is_empty());
    // DISTINCT pair of source (t_0) and final-target (t_1) identity columns.
    assert!(sql.contains("SELECT DISTINCT"), "got: {sql}");
    assert!(sql.contains(r#"t_0."id""#), "source identity projected: {sql}");
    assert!(sql.contains(r#"t_1."order_id""#), "target identity projected: {sql}");
    assert!(sql.contains("JOIN"), "joins present: {sql}");
}
```

(Verify the exact public names: `ChainType`, `DuckDbDialect`, and `compile_chain_pairs` must be exported from `query_api::sql`. Check the existing `compile_chain` test/import style with `grep -nE "use query_api::sql|DuckDbDialect|pub use|pub struct ChainType" src/services/query-api/tests/*.rs src/services/query-api/src/sql.rs` and mirror it — if the dialect type or import path differs, match the existing chain-compiler test.)

Add the BUCK target (mirror an existing pure-logic query-api `rust_test`, e.g. the chain compiler's). In `src/services/query-api/BUCK`:

```python
rust_test(
    name = "compile-chain-pairs",
    crate = "compile_chain_pairs",
    srcs = ["tests/compile_chain_pairs.rs"],
    crate_root = "tests/compile_chain_pairs.rs",
    edition = "2024",
    deps = [
        ":query-api",
        "//src/control-plane/core:core",
    ],
)
```

- [ ] **Step 2: Run it to verify it fails**

Run: `buck2 test //src/services/query-api:compile-chain-pairs > /tmp/t.log 2>&1; grep -nE "Tests finished|FAIL|error\[" /tmp/t.log`
Expected: FAIL (compile error — `compile_chain_pairs` undefined).

- [ ] **Step 3: Extract the shared FROM/WHERE builder**

In `src/services/query-api/src/sql.rs`, extract the FROM-clause + WHERE-conjuncts assembly shared by both chain compilers. Add this private helper (it reproduces the existing `compile_chain_with` body's `from`/`conjuncts`/`params` construction verbatim — same `t_{i}` aliasing, same hop joins, same per-position predicate+row-filter order):

```rust
/// Build the shared FROM clause (final target, then JOIN each predecessor down to the
/// source) and the per-position WHERE conjuncts (caller predicates then ACL row-filters,
/// bound at alias `t_i`) for a chain. The projection differs per caller. Row filters are
/// validated up front so `filter_sql` cannot panic.
fn chain_from_where(
    dialect: &dyn SqlDialect,
    types: &[ChainType],
    hops: &[LinkBacking],
) -> Result<(String, Vec<String>, Vec<SqlValue>), CompileError> {
    debug_assert_eq!(types.len(), hops.len() + 1, "chain types must be hops + 1");
    for t in types {
        for f in &t.row_filters {
            validate_row_filter(f, None).map_err(CompileError::MalformedFilter)?;
        }
    }
    let k = hops.len();
    let alias = |i: usize| format!("t_{i}");
    let tbl = |t: &TableRef| {
        format!(
            "{}.{}",
            dialect.quote_ident(&t.schema),
            dialect.quote_ident(&t.name)
        )
    };

    let mut from = format!("{} {}", tbl(&types[k].table), alias(k));
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
                    dialect.quote_ident(from_column),
                    dialect.quote_ident(to_column),
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
                    dialect.quote_ident(to_column),
                    dialect.quote_ident(to_key),
                    dialect.quote_ident(from_key),
                    dialect.quote_ident(from_column),
                ));
            }
        }
    }

    let mut params = Vec::new();
    let mut conjuncts: Vec<String> = Vec::new();
    for (i, t) in types.iter().enumerate() {
        let a = alias(i);
        for p in &t.predicates {
            conjuncts.push(caller_predicate_sql(dialect, p, &a, &mut params));
        }
        for f in &t.row_filters {
            conjuncts.push(filter_sql(dialect, f, &a, &mut params));
        }
    }
    Ok((from, conjuncts, params))
}
```

Then rewrite `compile_chain_with` to use it — replace its body from the `debug_assert_eq!` through the `conjuncts` loop with a call to the helper, keeping the final-target projection and assembly:

```rust
pub fn compile_chain_with(
    dialect: &dyn SqlDialect,
    types: &[ChainType],
    hops: &[LinkBacking],
    allowed_cols: &[String],
    mask_cols: &[String],
    limit: u32,
) -> Result<(String, Vec<SqlValue>), CompileError> {
    let k = hops.len();
    let final_alias = format!("t_{k}");
    let cols = allowed_cols
        .iter()
        .map(|c| {
            if mask_cols.iter().any(|m| m == c) {
                format!("'{MASK_MARKER}' AS {}", dialect.quote_ident(c))
            } else {
                format!("{final_alias}.{}", dialect.quote_ident(c))
            }
        })
        .collect::<Vec<_>>()
        .join(", ");
    let (from, conjuncts, params) = chain_from_where(dialect, types, hops)?;
    let mut sql = format!("SELECT DISTINCT {cols} FROM {from}");
    if !conjuncts.is_empty() {
        sql.push_str(" WHERE ");
        sql.push_str(&conjuncts.join(" AND "));
    }
    sql.push_str(&format!(" {}", dialect.limit_clause(limit)));
    Ok((sql, params))
}
```

- [ ] **Step 4: Add `compile_chain_pairs`**

In `src/services/query-api/src/sql.rs`, after `compile_chain_with`:

```rust
/// Compile a governed chain that projects exactly the source (`t_0`) and final-target
/// (`t_k`) identity columns as a DISTINCT pair, through the same governed joins/filters as
/// [`compile_chain_with`]. `source_id`/`target_id` are the identity property names (=
/// physical columns) of the source and final-target types.
pub fn compile_chain_pairs(
    dialect: &dyn SqlDialect,
    types: &[ChainType],
    hops: &[LinkBacking],
    source_id: &str,
    target_id: &str,
    limit: u32,
) -> Result<(String, Vec<SqlValue>), CompileError> {
    let k = hops.len();
    let cols = format!(
        "t_0.{}, t_{k}.{}",
        dialect.quote_ident(source_id),
        dialect.quote_ident(target_id),
    );
    let (from, conjuncts, params) = chain_from_where(dialect, types, hops)?;
    let mut sql = format!("SELECT DISTINCT {cols} FROM {from}");
    if !conjuncts.is_empty() {
        sql.push_str(" WHERE ");
        sql.push_str(&conjuncts.join(" AND "));
    }
    sql.push_str(&format!(" {}", dialect.limit_clause(limit)));
    Ok((sql, params))
}
```

- [ ] **Step 5: Run the compiler unit test + the existing chain tests**

Run: `buck2 test //src/services/query-api:compile-chain-pairs > /tmp/t.log 2>&1; grep -nE "Tests finished|Pass [0-9]|FAIL|error\[" /tmp/t.log`
Expected: PASS.
Run the existing chain-compiler test to confirm the refactor didn't regress it (find it: `grep -nE "compile_chain|chain" src/services/query-api/BUCK`), e.g.:
Run: `buck2 test //src/services/query-api:sql > /tmp/t2.log 2>&1; grep -nE "Tests finished|Pass [0-9]|FAIL" /tmp/t2.log`
Expected: PASS (no regression).

- [ ] **Step 6: Extract `resolve_chain` in the handler**

In `src/services/query-api/src/handler.rs`, extract the chain-resolution body of `read_linked_chain` (from the depth check through the caller-filter loop, producing `metas`, `ctypes`, `hops`) into a private async helper, and rewrite `read_linked_chain` to call it. Add:

```rust
/// Resolve + govern a chain: depth check, source Read gate, per-hop type resolution
/// (forward/inverse) with Read-on-every-reached-type, per-position row-filters, and
/// caller-filter coercion/visibility. Returns the per-position metadata, the compiler
/// `ChainType`s (row-filters + caller predicates), and the hop backings. Shared by the
/// object-projection read and the association read so governance lives in one place.
async fn resolve_chain(
    q: &ChainQuery,
    subject: &Subject,
    deps: &QueryDeps<'_>,
) -> Result<(Vec<HopMeta>, Vec<crate::sql::ChainType>, Vec<control_plane_core::LinkBacking>), QueryError> {
    // (move the existing read_linked_chain body here, verbatim, from the
    //  `if q.path.is_empty() …` depth check through the end of the
    //  `for f in &q.filters { … }` caller-filter loop, then:)
    Ok((metas, ctypes, hops))
}
```

Rewrite `read_linked_chain` to:

```rust
pub async fn read_linked_chain(
    q: &ChainQuery,
    subject: &Subject,
    deps: &QueryDeps<'_>,
) -> Result<ObjectRows, QueryError> {
    let (metas, ctypes, hops) = resolve_chain(q, subject, deps).await?;
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
    let (sql, params) = compile_chain_with(
        deps.serving.dialect(),
        &ctypes,
        &hops,
        &to_allowed,
        &to_mask_cols,
        DEFAULT_LIMIT,
    )?;
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

Add the import for `compile_chain_with` if the `use crate::sql::{…}` line doesn't already cover it (it imports `compile_chain_with, compile_select_with` already — keep it).

- [ ] **Step 7: Add `NoIdentity`, `Associations`, and `read_associations`**

In `src/services/query-api/src/handler.rs`, add the error variant to `QueryError`:

```rust
    /// Association was requested but a projected end (source or final target) has no
    /// declared identity, so its objects cannot be named in a pair.
    #[error("type has no declared identity: {0}")]
    NoIdentity(String),
```

Add the result type and read function (place near `read_linked_chain`):

```rust
/// A governed source→target association result: deduped identity pairs plus the logical
/// type of each end's identity (for typed rendering).
#[derive(Debug)]
pub struct Associations {
    pub from_id_type: String,
    pub to_id_type: String,
    pub pairs: Vec<(SqlValue, SqlValue)>,
}

/// A governed traversal returning source↔final-target identity pairs (the edge list)
/// instead of the projected target objects. Resolves + governs the chain identically to
/// `read_linked_chain`, then requires a declared, caller-visible identity on the source
/// and final-target types and projects the two id columns as a DISTINCT pair.
pub async fn read_associations(
    q: &ChainQuery,
    subject: &Subject,
    deps: &QueryDeps<'_>,
) -> Result<Associations, QueryError> {
    let (metas, ctypes, hops) = resolve_chain(q, subject, deps).await?;
    let source = &metas[0];
    let target = metas.last().expect("non-empty path yields a final target");

    // Both projected ends must declare an identity.
    let source_id = source
        .otype
        .identity
        .clone()
        .ok_or_else(|| QueryError::NoIdentity(source.otype.name.0.clone()))?;
    let target_id = target
        .otype
        .identity
        .clone()
        .ok_or_else(|| QueryError::NoIdentity(target.otype.name.0.clone()))?;

    // …and the identity column must be visible (not denied, not masked) on each end —
    // you cannot associate objects you cannot identify.
    let s_allowed = project_allowed(&source.otype.properties, &source.denied);
    if !s_allowed.contains(&source_id) || source.masked.contains(&source_id) {
        return Err(QueryError::Forbidden);
    }
    let t_allowed = project_allowed(&target.otype.properties, &target.denied);
    if !t_allowed.contains(&target_id) || target.masked.contains(&target_id) {
        return Err(QueryError::Forbidden);
    }

    let from_id_type = source
        .otype
        .properties
        .iter()
        .find(|p| p.name == source_id)
        .map(|p| p.ty.clone())
        .unwrap_or_default();
    let to_id_type = target
        .otype
        .properties
        .iter()
        .find(|p| p.name == target_id)
        .map(|p| p.ty.clone())
        .unwrap_or_default();

    let (sql, params) = crate::sql::compile_chain_pairs(
        deps.serving.dialect(),
        &ctypes,
        &hops,
        &source_id,
        &target_id,
        DEFAULT_LIMIT,
    )?;
    let served = deps.serving.fetch_rows(&sql, &params).await?;
    let pairs: Vec<(SqlValue, SqlValue)> = served
        .rows
        .into_iter()
        .map(|mut row| {
            // compile_chain_pairs SELECTs exactly [source_id, target_id] in that order.
            let to = row.pop().unwrap_or(SqlValue::Null);
            let from = row.pop().unwrap_or(SqlValue::Null);
            (from, to)
        })
        .collect();
    Ok(Associations {
        from_id_type,
        to_id_type,
        pairs,
    })
}
```

Export it from the crate: in `src/services/query-api/src/lib.rs`, add `read_associations` and `Associations` to the existing handler re-export (find the `pub use handler::{…}` line and extend it).

- [ ] **Step 8: Write the handler test on memory (failing → passing)**

Create `src/services/query-api/tests/associations.rs`. Mirror an existing in-memory handler test (e.g. `link_traversal.rs` or `multi_hop_traversal_e2e.rs` — check which uses `MemoryControlPlane` + a fake `ServingEngine`, and reuse its harness). The test: define a source type (identity `id`) and a target type (identity `order_id`) linked by an FK, seed the fake serving engine to return id pairs, call `read_associations`, assert the returned `pairs` and `from_id_type`/`to_id_type`. Also assert the `NoIdentity` path: a source type with `identity: None` → `Err(QueryError::NoIdentity(_))`.

(Adapt to whatever fake `ServingEngine` the existing query-api tests use. If the in-memory tests drive a stub serving engine, assert on `pairs`; if they require DuckDB, defer the full data assertion to Task 4's e2e and keep this test to the `NoIdentity`/`Forbidden` governance paths, which need no serving round-trip. Inspect `grep -rnE "ServingEngine|fetch_rows|struct .*Serving" src/services/query-api/tests/*.rs` and match the established pattern.)

Add the BUCK target (mirror the existing handler test's target shape — `rust_test` if it uses a fake serving engine, `loom_fixture_test` with `duckdb = True` if it needs DuckDB):

```python
rust_test(
    name = "associations",
    crate = "associations",
    srcs = ["tests/associations.rs"],
    crate_root = "tests/associations.rs",
    edition = "2024",
    deps = [
        ":query-api",
        "//src/control-plane/core:core",
        "//src/control-plane/memory:memory",
    ],
)
```

(Add any other deps the chosen harness needs — copy them from the existing handler test's target.)

- [ ] **Step 9: Run it + the full query-api suite + clippy**

Run: `buck2 test //src/services/query-api:associations > /tmp/t.log 2>&1; grep -nE "Tests finished|Pass [0-9]|FAIL|panicked" /tmp/t.log`
Expected: PASS.
Run: `buck2 test //src/services/query-api/... > /tmp/t2.log 2>&1; grep -nE "Tests finished|Pass [0-9]|FAIL|panicked" /tmp/t2.log`
Expected: all pass (the `resolve_chain` refactor preserved existing traversal behavior).
Run: `tools/clippy-all.sh > /tmp/c.log 2>&1; grep -nE "warning|error" /tmp/c.log || echo CLEAN`
Expected: CLEAN.

- [ ] **Step 10: Commit**

```bash
git add src/services/query-api/
git commit -m "feat(query): read_associations — governed source->target id pairs

Extract a shared chain resolver (governance in one place) and a shared FROM/WHERE
builder; add compile_chain_pairs (projects the source t_0 and final-target t_k
identity columns as a DISTINCT pair) and read_associations, which requires a
declared, caller-visible identity on both projected ends (else NoIdentity/Forbidden).

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 4: Association HTTP surface + render + e2e

**Files:**
- Modify: `src/services/query-api/src/render.rs` (`associations_to_json`)
- Modify: `src/services/query-api/src/http.rs` (`?shape=association` on both chain routes; `NoIdentity`→400)
- Create: `src/services/query-api/tests/association_e2e.rs` (fixture + DuckDB) + BUCK target

**Interfaces:**
- Consumes: `Associations`, `read_associations`, `QueryError::NoIdentity` (Task 3).
- Produces: `associations_to_json(&Associations) -> serde_json::Value` → `{ "associations": [ { "from": …, "to": … }, … ] }`; `?shape=association` on `/objects/:from/links/:link` and `/objects/:from/links`.

- [ ] **Step 1: Add the renderer**

In `src/services/query-api/src/render.rs`, add (reusing the private `render_cell`):

```rust
use crate::handler::Associations;

/// `{ "associations": [ { "from": <typed id>, "to": <typed id> }, ... ] }`. Each id is
/// rendered by its identity property's logical type.
pub fn associations_to_json(a: &Associations) -> Value {
    let assocs: Vec<Value> = a
        .pairs
        .iter()
        .map(|(from, to)| {
            json!({
                "from": render_cell(&a.from_id_type, from),
                "to": render_cell(&a.to_id_type, to),
            })
        })
        .collect();
    json!({ "associations": assocs })
}
```

- [ ] **Step 2: Wire `?shape=association` into the single-hop route**

In `src/services/query-api/src/http.rs` `get_linked`, pull `shape` out of params alongside `direction` (extend the param loop), parse it, and branch the terminal call. Change the param-splitting loop and the match:

```rust
    let mut direction_raw: Option<String> = None;
    let mut shape: Option<String> = None;
    let mut filter_params: Vec<(String, String)> = Vec::with_capacity(params.len());
    for (k, v) in params {
        match k.as_str() {
            "direction" => direction_raw = Some(v),
            "shape" => shape = Some(v),
            _ => filter_params.push((k, v)),
        }
    }
```

After building `direction`, `filters`, and `deps`, validate `shape` and branch (replace the single `match read_linked_chain(...)` block):

```rust
    let query = ChainQuery {
        from_type,
        path: vec![Hop { link: link_name, direction }],
        filters,
    };
    let subj = Subject(SubjectId(subject));
    match shape.as_deref() {
        None | Some("objects") => respond_objects(read_linked_chain(&query, &subj, &deps).await),
        Some("association") => {
            respond_associations(read_associations(&query, &subj, &deps).await)
        }
        Some(other) => (StatusCode::BAD_REQUEST, format!("unknown shape: {other}")).into_response(),
    }
```

Add two small response helpers in `http.rs` (so both chain handlers share the error mapping):

```rust
fn respond_objects(res: Result<crate::handler::ObjectRows, QueryError>) -> axum::response::Response {
    match res {
        Ok(rows) => Json(crate::render::objects_to_json(&rows)).into_response(),
        Err(e) => chain_error(e),
    }
}

fn respond_associations(res: Result<crate::handler::Associations, QueryError>) -> axum::response::Response {
    match res {
        Ok(a) => Json(crate::render::associations_to_json(&a)).into_response(),
        Err(e) => chain_error(e),
    }
}

/// Shared HTTP mapping for chain/association read errors.
fn chain_error(e: QueryError) -> axum::response::Response {
    match e {
        QueryError::UnknownType(t) => (StatusCode::NOT_FOUND, t).into_response(),
        QueryError::UnknownLink(l) => (StatusCode::NOT_FOUND, l).into_response(),
        QueryError::AmbiguousLink(l) => (StatusCode::BAD_REQUEST, l).into_response(),
        QueryError::BadChain(m) => (StatusCode::BAD_REQUEST, m).into_response(),
        QueryError::NoIdentity(t) => (StatusCode::BAD_REQUEST, t).into_response(),
        QueryError::Forbidden => StatusCode::FORBIDDEN.into_response(),
        QueryError::BadFilter(c) => (StatusCode::BAD_REQUEST, c).into_response(),
        _ => (StatusCode::INTERNAL_SERVER_ERROR, "internal error").into_response(),
    }
}
```

Update the imports at the top of `http.rs` to include `read_associations` and `Associations`:

```rust
use crate::handler::{
    Associations, ChainQuery, Hop, ObjectQuery, QueryDeps, QueryError, Subject, read_associations,
    read_linked_chain, read_object,
};
```

- [ ] **Step 3: Wire `?shape=association` into the multi-hop route**

In `get_linked_chain`, do the same: pull `shape` out alongside `path`, then branch via the shared helpers. Change the loop:

```rust
    let mut hops: Vec<Hop> = Vec::new();
    let mut shape: Option<String> = None;
    let mut filter_params: Vec<(String, String)> = Vec::with_capacity(params.len());
    for (k, v) in params {
        match k.as_str() {
            "path" => hops = parse_path_hops(&v),
            "shape" => shape = Some(v),
            _ => filter_params.push((k, v)),
        }
    }
```

and replace the terminal `match read_linked_chain(...)`:

```rust
    let query = ChainQuery { from_type, path: hops, filters };
    let subj = Subject(SubjectId(subject));
    match shape.as_deref() {
        None | Some("objects") => respond_objects(read_linked_chain(&query, &subj, &deps).await),
        Some("association") => respond_associations(read_associations(&query, &subj, &deps).await),
        Some(other) => (StatusCode::BAD_REQUEST, format!("unknown shape: {other}")).into_response(),
    }
```

(Remove the now-duplicated inline error arms from both handlers, since `chain_error` covers them.)

- [ ] **Step 4: Build the query-api crate**

Run: `buck2 build //src/services/query-api:query-api > /tmp/b.log 2>&1; grep -nE "BUILD SUCCEEDED|error\[" /tmp/b.log`
Expected: BUILD SUCCEEDED.

- [ ] **Step 5: Write the e2e (failing)**

Create `src/services/query-api/tests/association_e2e.rs`. Mirror the existing multi-hop / link-traversal e2e (`multi_hop_traversal_e2e.rs`) end-to-end harness: boot the fixture, land source + target tables, define types **with identities**, define the link(s), grant Read, then drive the HTTP router (or `read_associations` directly through the real DuckDB serving engine). Assertions:

- single-hop `?shape=association` returns the exact `{from, to}` id pairs for the seeded data;
- multi-hop association pairs the source identity with the final-target identity;
- a row-filter (or caller filter) on an intermediate/target position drops the pairs routing through excluded rows;
- dedup: the same source reaching the same target by two paths yields one pair; different sources reaching one target yield distinct pairs;
- associating a type whose source or target has no declared identity → HTTP 400 (`NoIdentity`).

(Reuse the established fixture helpers — `PgFixture`, `DuckLakeWriter`, the router/`AppState` setup, the `X-Loom-Subject` header, and the ACL grant calls — from `multi_hop_traversal_e2e.rs`; copy its imports/scaffold and change the type definitions to declare identities and the request to add `shape=association`. The JSON shape to assert is `{"associations":[{"from":…,"to":…}]}`.)

Add the BUCK target (mirror `multi_hop_traversal_e2e`'s `loom_fixture_test` with `duckdb = True`, copying its `deps`):

```python
loom_fixture_test(
    name = "association-e2e",
    crate = "association_e2e",
    srcs = ["tests/association_e2e.rs"],
    crate_root = "tests/association_e2e.rs",
    duckdb = True,
    deps = [
        # copy verbatim from the multi-hop-traversal-e2e target's deps
    ],
)
```

- [ ] **Step 6: Run the e2e to verify it passes**

Run: `buck2 test //src/services/query-api:association-e2e > /tmp/t.log 2>&1; grep -nE "Tests finished|Pass [0-9]|FAIL|panicked|error\[" /tmp/t.log`
Expected: PASS.

- [ ] **Step 7: Full query-api suite + clippy**

Run: `buck2 test //src/services/query-api/... > /tmp/t.log 2>&1; grep -nE "Tests finished|Pass [0-9]|FAIL|panicked" /tmp/t.log`
Expected: all pass.
Run: `tools/clippy-all.sh > /tmp/c.log 2>&1; grep -nE "warning|error" /tmp/c.log || echo CLEAN`
Expected: CLEAN.

- [ ] **Step 8: Commit**

```bash
git add src/services/query-api/
git commit -m "feat(query): ?shape=association HTTP surface + render

A shape=association flag on the existing /objects/:from/links{,/:link} routes
returns {associations:[{from,to}]} typed id pairs via read_associations; default
shape is unchanged. NoIdentity -> 400. Shared chain_error mapping across both
routes. Proven by a DuckDB-backed e2e (single/multi-hop, governance, dedup, 400).

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 5: Docs

**Files:**
- Modify: `docs/superpowers/specs/2026-06-06-loom-roadmap.md`
- Modify: `docs/FUTURE.md`

- [ ] **Step 1: Roadmap**

In `docs/superpowers/specs/2026-06-06-loom-roadmap.md`, add a "Where we are" paragraph recording the delivery: object identity is now a first-class `ObjectType` field (validated at bind), and `?shape=association` returns governed source→target identity pairs (the edge list) over the chain read path. Note the remaining deferred slice-C piece is **object-set-by-identity input** (`?ids=`). If the slice-C parts are enumerated in a "remaining" sentence, update it so only object-set input (and any genuinely-remaining items) stays listed.

- [ ] **Step 2: FUTURE.md**

In `docs/FUTURE.md`, read it first, then record (editing the relevant query/reads area in place): source→target association delivered (compact id-pairs, governed both-ends, on `?shape=association`); object identity now first-class; the remaining graph follow-up is **object-set inputs keyed on identity** (`?ids=1,2,3` → `in:` on the source identity), plus the longer-standing `/graph` surface for cyclic/self-link traversal.

- [ ] **Step 3: Lint + commit**

Run: `buck2 run //tools:prek -- run --all-files > /tmp/p.log 2>&1; grep -nE "Failed|Passed|error" /tmp/p.log | tail -20`
Expected: all hooks pass (fix any markdown trailing-whitespace / EOF-newline the hooks flag, then re-run).

```bash
git add docs/
git commit -m "docs(query): mark object identity + source->target association delivered

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
