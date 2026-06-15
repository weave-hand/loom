# Derived Properties — Part 1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add aggregate-over-link **derived properties** (`Customer.orderCount = COUNT` of linked Orders, `totalSpend = SUM` over them) to object types, served through `read_object` as governed correlated subqueries.

**Architecture:** A new `ObjectType.derived: Vec<DerivedPropertyDef>` (link name + `COUNT`/`SUM`/`AVG`/`MIN`/`MAX`) stored in a new `ontology.derived_property` table. `compile_select` gains a `derived` parameter compiling each aggregate as a correlated subquery reusing the link's FK/join-table join shape. `read_object` resolves + governs each derived property both-ends (subject needs `Read` on the linked type; its row-filters apply inside the subquery; omitted like a denied column otherwise).

**Tech Stack:** Rust 2024, buck2, DuckDB-over-DuckLake serving, Postgres control plane (sqlx compile-time SQL), `loom_fixture_test`. Tests are `rust_test`/`loom_fixture_test` targets — never inline `#[cfg(test)]`.

**Spec:** `docs/superpowers/specs/2026-06-15-derived-properties-design.md`

**Key design choices (baked in):**
- `DerivedAggregate`/`DerivedSelect` (the compiler inputs) are **owned** types (clone the resolved `Aggregation`/`LinkBacking`/`TableRef`/`RowFilter`s) — no borrow/lifetime threading in the handler; clone cost is trivial on a read path.
- `compile_select` gains a `derived: &[DerivedSelect]` param **before** `limit`; existing callers pass `&[]`.
- The outer table is aliased `o` **only when** ≥1 aggregate is present, so the existing no-derived SQL (and its unit tests) stay byte-identical.

---

## File Structure

| File | Responsibility | Action |
|------|----------------|--------|
| `src/control-plane/core/src/ontology.rs` | `Aggregation`, `DerivedPropertyDef`, `ObjectType.derived` | Modify |
| `src/control-plane/core/src/lib.rs` | exports | Modify |
| (all `ObjectType { … }` literals in src + tests) | add `derived: vec![]` | Modify (mechanical) |
| `src/control-plane/postgres/migrations/00NN_derived_property.sql` | `ontology.derived_property` table | Create |
| `src/control-plane/postgres/src/ontology.rs` | persist/read `derived` in `define_type`/`get_type` | Modify |
| `src/control-plane/postgres/.sqlx/` | regenerated cache | Modify (generated) |
| `src/control-plane/memory/src/ontology.rs` | store `derived` on the cloned `ObjectType` (no change if it clones the whole type) | Verify/Modify |
| `src/control-plane/testkit/src/lib.rs` | derived round-trip in `ontology_contract` | Modify |
| `src/services/query-api/src/sql.rs` | `DerivedAggregate`/`DerivedSelect` + `compile_select` extension + `derived_aggregate_sql` | Modify |
| `src/services/query-api/tests/sql_compile.rs` | update existing calls (`&[]`) + new derived compile tests | Modify |
| `src/services/query-api/src/handler.rs` | `read_object` derived resolution + both-ends governance | Modify |
| `src/services/query-api/tests/derived_properties_e2e.rs` | governed e2e | Create |
| `src/services/query-api/BUCK` | e2e target | Modify |
| roadmap, `docs/FUTURE.md` | delivered marker + follow-ups | Modify |

---

## Task 1: Core `DerivedPropertyDef` + `ObjectType.derived` (+ fix all construction sites)

Add the types and the new field. Adding a field to `ObjectType` breaks every `ObjectType { … }` literal in the codebase — fix them all to `derived: vec![]` (the compiler enumerates them). This task compiles with `derived` always empty; storage comes in Task 2.

**Files:**
- Modify: `src/control-plane/core/src/ontology.rs`, `src/control-plane/core/src/lib.rs`
- Modify (mechanical): every file constructing `ObjectType { … }`

- [ ] **Step 1: Add the core types**

In `src/control-plane/core/src/ontology.rs`, after `LinkDef` (before the `Ontology` trait), add:

```rust
/// How a derived property aggregates over its link's target rows. The `String` is the
/// target-type column to aggregate (COUNT takes none).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Aggregation {
    Count,
    Sum(String),
    Avg(String),
    Min(String),
    Max(String),
}

/// A computed property: aggregate `agg` over the rows reachable from this type via the
/// link named `link`. `ty` is the declared logical type of the result (e.g. "Long" for a
/// count, "Double" for an average).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DerivedPropertyDef {
    pub name: String,
    pub ty: String,
    pub link: String,
    pub agg: Aggregation,
}
```

Add the `derived` field to `ObjectType` (after `properties`):

```rust
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObjectType {
    pub name: TypeName,
    /// Ordered.
    pub properties: Vec<PropertyDef>,
    /// Ordered. Aggregate-over-link computed properties (served alongside `properties`).
    pub derived: Vec<DerivedPropertyDef>,
    pub table: TableRef,
}
```

- [ ] **Step 2: Export from lib.rs**

In `src/control-plane/core/src/lib.rs`, add `Aggregation, DerivedPropertyDef` to the `pub use ontology::{...}` line (keep order tidy).

- [ ] **Step 3: Fix every `ObjectType` construction site**

Build to enumerate them: `buck2 build //src/... 2>&1 | grep -E "missing field .derived|ObjectType" | head -40`. Add `derived: vec![]` to EVERY `ObjectType { name, properties, table }` literal (in `src/` and all `tests/`). Known sites (verify with the compiler, there may be more): `src/control-plane/postgres/src/ontology.rs` (`get_type`), `src/control-plane/memory/*` if it constructs, `src/control-plane/testkit/src/lib.rs`, `src/services/ingest/src/bind.rs`, and test files: `query-api/tests/{bind_read_e2e,governed_read,link_traversal,...}.rs`, `transform/tests/{typed_transform_e2e}.rs`, `query-api/tests/action_e2e.rs`. Place `derived: vec![]` consistently after `properties` (matching the struct field order is not required by Rust, but keep it tidy).

- [ ] **Step 4: Build + lint**

Run: `buck2 build //src/... 2>&1 | tail -15` — expect clean (all literals fixed).
Run: `buck2 build '//src/control-plane/core:core[clippy.txt]' 2>&1 | tail -5` — expect empty.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat(core): ObjectType.derived + DerivedPropertyDef/Aggregation (empty everywhere)"
```

---

## Task 2: Postgres + memory storage for `derived`

Persist and read the `derived` collection.

**Files:**
- Create: `src/control-plane/postgres/migrations/00NN_derived_property.sql`
- Modify: `src/control-plane/postgres/src/ontology.rs`, `src/control-plane/memory/src/ontology.rs` (verify)
- Modify (generated): `src/control-plane/postgres/.sqlx/`

- [ ] **Step 1: Migration**

List `src/control-plane/postgres/migrations/` and create the next sequential `00NN_derived_property.sql`:

```sql
create table ontology.derived_property (
    type_name  text    not null references ontology.object_type (name) on delete cascade,
    ordinal    int     not null,
    name       text    not null,
    ty         text    not null,
    link_name  text    not null,
    agg_kind   text    not null,
    agg_column text,
    primary key (type_name, ordinal)
);
```

- [ ] **Step 2: Postgres adapter — write derived in `define_type`**

In `src/control-plane/postgres/src/ontology.rs`, read the existing `define_type` (it upserts `object_type`, deletes+reinserts `property` rows in a transaction). Add a delete + per-derived insert in the SAME transaction, after the property loop and before `tx.commit()`. A small helper maps `Aggregation` → `(kind, column)`:

```rust
fn agg_parts(a: &Aggregation) -> (&'static str, Option<&str>) {
    match a {
        Aggregation::Count => ("count", None),
        Aggregation::Sum(c) => ("sum", Some(c.as_str())),
        Aggregation::Avg(c) => ("avg", Some(c.as_str())),
        Aggregation::Min(c) => ("min", Some(c.as_str())),
        Aggregation::Max(c) => ("max", Some(c.as_str())),
    }
}
```

Insert block (mirror the property loop's style):

```rust
    sqlx::query!(
        "delete from ontology.derived_property where type_name = $1",
        ty.name.0,
    )
    .execute(&mut *tx)
    .await
    .map_err(backend)?;
    for (i, d) in ty.derived.iter().enumerate() {
        let (kind, column) = agg_parts(&d.agg);
        sqlx::query!(
            "insert into ontology.derived_property \
             (type_name, ordinal, name, ty, link_name, agg_kind, agg_column) \
             values ($1, $2, $3, $4, $5, $6, $7)",
            ty.name.0,
            i as i32,
            d.name,
            d.ty,
            d.link,
            kind,
            column,
        )
        .execute(&mut *tx)
        .await
        .map_err(backend)?;
    }
```

- [ ] **Step 3: Postgres adapter — read derived in `get_type`**

In `get_type`, after fetching `props`, fetch the derived rows and reconstruct. A helper rebuilds `Aggregation`:

```rust
fn rebuild_agg(kind: &str, column: Option<String>) -> Result<Aggregation, ControlPlaneError> {
    let col = || column.clone().ok_or_else(|| {
        ControlPlaneError::Backend(format!("derived agg '{kind}' missing column").into())
    });
    Ok(match kind {
        "count" => Aggregation::Count,
        "sum" => Aggregation::Sum(col()?),
        "avg" => Aggregation::Avg(col()?),
        "min" => Aggregation::Min(col()?),
        "max" => Aggregation::Max(col()?),
        other => {
            return Err(ControlPlaneError::Backend(
                format!("unknown derived agg kind '{other}'").into(),
            ));
        }
    })
}
```

Fetch + build (mirror the props fetch):

```rust
    let derived_rows = sqlx::query!(
        "select name, ty, link_name, agg_kind, agg_column from ontology.derived_property \
         where type_name = $1 order by ordinal",
        name.0,
    )
    .fetch_all(&self.pool)
    .await
    .map_err(backend)?;
    let mut derived = Vec::with_capacity(derived_rows.len());
    for r in derived_rows {
        derived.push(DerivedPropertyDef {
            name: r.name,
            ty: r.ty,
            link: r.link_name,
            agg: rebuild_agg(&r.agg_kind, r.agg_column)?,
        });
    }
```

Then add `derived,` to the `ObjectType { … }` it returns (replacing the `derived: vec![]` Task 1 left there). Add `Aggregation, DerivedPropertyDef` to this file's `use control_plane_core::{…}` import. (`ControlPlaneError::Backend` takes a `Box<dyn Error + Send + Sync>`; `format!(...).into()` works via `String: Into<Box<…>>` — confirm against the existing `backend` helper's error type; if `Backend` wraps differently, mirror how other adapter errors are built.)

- [ ] **Step 4: Regenerate `.sqlx`**

Run: `tools/sqlx-prepare.sh`. Confirm new `query-*.json` files: `git status src/control-plane/postgres/.sqlx | head`.

- [ ] **Step 5: Memory fake**

Read `src/control-plane/memory/src/ontology.rs`. `define_type` stores the whole `ObjectType` (clone) into a map and `get_type` clones it back, so `derived` round-trips automatically — **verify** this (the memory `OntologyState.types` holds `ObjectType` directly). If so, NO change is needed; note it. If the memory fake reconstructs `ObjectType` field-by-field anywhere, add `derived`.

- [ ] **Step 6: Build + lint + commit**

Run: `buck2 build //src/control-plane/... 2>&1 | tail -10` (clean).
Run: `buck2 build '//src/control-plane/postgres:postgres[clippy.txt]' 2>&1 | tail -5` (empty).

```bash
git add src/control-plane/postgres/ src/control-plane/memory/
git commit -m "feat(control-plane): persist ObjectType.derived (pg + memory)"
```

---

## Task 3: Testkit derived round-trip

**Files:**
- Modify: `src/control-plane/testkit/src/lib.rs`

- [ ] **Step 1: Add assertions**

In `ontology_contract`, after the existing type/action assertions, append (reuse `tn`/`tref`; the `Widget` type defined earlier in the contract — if not, define one first):

```rust
    // --- Derived properties ---
    o.define_type(ObjectType {
        name: tn("Account"),
        properties: vec![PropertyDef { name: "id".into(), ty: "Long".into(), required: true }],
        derived: vec![
            DerivedPropertyDef {
                name: "txnCount".into(),
                ty: "Long".into(),
                link: "transactions".into(),
                agg: Aggregation::Count,
            },
            DerivedPropertyDef {
                name: "balance".into(),
                ty: "Double".into(),
                link: "transactions".into(),
                agg: Aggregation::Sum("amount".into()),
            },
        ],
        table: tref("main", "account"),
    })
    .await
    .expect("define Account with derived");
    let got = o.get_type(&tn("Account")).await.unwrap();
    assert_eq!(
        got.derived.iter().map(|d| d.name.clone()).collect::<Vec<_>>(),
        vec!["txnCount".to_string(), "balance".to_string()],
        "derived order preserved"
    );
    assert_eq!(got.derived[0].agg, Aggregation::Count);
    assert_eq!(got.derived[1].agg, Aggregation::Sum("amount".into()));
    // Redefine with fewer derived -> replaced.
    o.define_type(ObjectType {
        name: tn("Account"),
        properties: vec![PropertyDef { name: "id".into(), ty: "Long".into(), required: true }],
        derived: vec![],
        table: tref("main", "account"),
    })
    .await
    .unwrap();
    assert!(o.get_type(&tn("Account")).await.unwrap().derived.is_empty(), "redefine replaces derived");
```

Add `Aggregation`, `DerivedPropertyDef` to the testkit's `use control_plane_core::{…}`.

- [ ] **Step 2: Run both adapters**

Run: `buck2 test //src/control-plane/memory:ontology //src/control-plane/postgres:ontology > /tmp/t3.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t3.log` → `Pass 2. Fail 0.`

- [ ] **Step 3: Commit**

```bash
git add src/control-plane/testkit/src/lib.rs
git commit -m "test(testkit): derived-property round-trip contract (both adapters)"
```

---

## Task 4: `compile_select` extension — correlated-aggregate SQL

**Files:**
- Modify: `src/services/query-api/src/sql.rs`
- Modify: `src/services/query-api/tests/sql_compile.rs`

- [ ] **Step 1: Add the compiler input types + helper**

In `src/services/query-api/src/sql.rs`, add `Aggregation` to the `use control_plane_core::{…}` import. Add, above `compile_select`:

```rust
/// An aggregate-over-link derived property to compile into a correlated subquery in the
/// SELECT. Owned (the handler clones the resolved link/target/filters into it).
pub struct DerivedAggregate {
    pub name: String,
    pub agg: Aggregation,
    pub backing: LinkBacking,
    pub target_table: TableRef,
    /// The linked type's ACL row-filters (both-ends governance), applied inside the subquery.
    pub target_filters: Vec<RowFilter>,
}

/// A derived-property SELECT expression: either a computed aggregate, or a masked marker
/// (the property is visible-but-masked — emit `'***'`, never the aggregate).
pub enum DerivedSelect {
    Masked(String),
    Aggregate(DerivedAggregate),
}

/// Build the correlated-subquery SQL for one derived aggregate, pushing its target-filter
/// params (in order) onto `params`. The outer object table is aliased `o`.
fn derived_aggregate_sql(d: &DerivedAggregate, params: &mut Vec<SqlValue>) -> String {
    let target = format!(
        "{}.{}",
        quote_ident(&d.target_table.schema),
        quote_ident(&d.target_table.name)
    );
    let aggfn = match &d.agg {
        Aggregation::Count => "COUNT(*)".to_string(),
        Aggregation::Sum(c) => format!("COALESCE(SUM(sub.{}), 0)", quote_ident(c)),
        Aggregation::Avg(c) => format!("AVG(sub.{})", quote_ident(c)),
        Aggregation::Min(c) => format!("MIN(sub.{})", quote_ident(c)),
        Aggregation::Max(c) => format!("MAX(sub.{})", quote_ident(c)),
    };
    let (from_join, correlation) = match &d.backing {
        LinkBacking::ForeignKey { from_column, to_column } => (
            format!("{target} sub"),
            format!("sub.{} = o.{}", quote_ident(to_column), quote_ident(from_column)),
        ),
        LinkBacking::JoinTable { table, from_key, from_column, to_column, to_key } => {
            let jt = format!("{}.{}", quote_ident(&table.schema), quote_ident(&table.name));
            (
                format!(
                    "{target} sub JOIN {jt} j ON j.{} = sub.{}",
                    quote_ident(to_column),
                    quote_ident(to_key)
                ),
                format!("j.{} = o.{}", quote_ident(from_column), quote_ident(from_key)),
            )
        }
    };
    let mut conjuncts = vec![correlation];
    for f in &d.target_filters {
        conjuncts.push(filter_sql(f, "sub", params));
    }
    format!(
        "(SELECT {aggfn} FROM {from_join} WHERE {}) AS {}",
        conjuncts.join(" AND "),
        quote_ident(&d.name)
    )
}
```

- [ ] **Step 2: Extend `compile_select`**

Replace the `compile_select` function with this version (adds the `derived` param before `limit`; derived params are pushed BEFORE the WHERE conjunct params; the outer table is aliased `o` only when an aggregate is present):

```rust
/// `allowed_cols` must be non-empty (caller enforces). `row_filters` and `eq_filters`
/// are ANDed together as conjuncts. `derived` aggregate subqueries (if any) are appended
/// to the SELECT list; their params precede the WHERE params.
#[allow(clippy::too_many_arguments)]
pub fn compile_select(
    table: &TableRef,
    allowed_cols: &[String],
    mask_cols: &[String],
    row_filters: &[RowFilter],
    eq_filters: &[(String, SqlValue)],
    derived: &[DerivedSelect],
    limit: u32,
) -> Result<(String, Vec<SqlValue>), CompileError> {
    // Validate every ACL filter shape up front — outer row filters and each derived
    // aggregate's target filters — so the SQL-building arms cannot hit a mismatch.
    for f in row_filters {
        validate_row_filter(f, None).map_err(CompileError::MalformedFilter)?;
    }
    for d in derived {
        if let DerivedSelect::Aggregate(a) = d {
            for f in &a.target_filters {
                validate_row_filter(f, None).map_err(CompileError::MalformedFilter)?;
            }
        }
    }

    let mut params = Vec::new();
    // Physical columns (no params).
    let mut col_exprs: Vec<String> = allowed_cols
        .iter()
        .map(|c| {
            if mask_cols.iter().any(|m| m == c) {
                format!("'{MASK_MARKER}' AS {}", quote_ident(c))
            } else {
                quote_ident(c)
            }
        })
        .collect();
    // Derived columns. Aggregate subquery params are pushed here — i.e. BEFORE the WHERE
    // params below — matching their left-to-right position in the SELECT clause.
    let has_aggregate = derived
        .iter()
        .any(|d| matches!(d, DerivedSelect::Aggregate(_)));
    for d in derived {
        match d {
            DerivedSelect::Masked(name) => {
                col_exprs.push(format!("'{MASK_MARKER}' AS {}", quote_ident(name)))
            }
            DerivedSelect::Aggregate(a) => col_exprs.push(derived_aggregate_sql(a, &mut params)),
        }
    }
    let cols = col_exprs.join(", ");

    // The outer table needs an alias only when a correlated subquery references it.
    let from = format!(
        "{}.{}",
        quote_ident(&table.schema),
        quote_ident(&table.name)
    );
    let from_clause = if has_aggregate { format!("{from} o") } else { from };

    let mut conjuncts: Vec<String> = Vec::new();
    for f in row_filters {
        conjuncts.push(filter_sql(f, "", &mut params));
    }
    for (col, val) in eq_filters {
        conjuncts.push(format!("({} = ?)", quote_ident(col)));
        params.push(val.clone());
    }

    let mut sql = format!("SELECT {cols} FROM {from_clause}");
    if !conjuncts.is_empty() {
        sql.push_str(" WHERE ");
        sql.push_str(&conjuncts.join(" AND "));
    }
    sql.push_str(&format!(" LIMIT {limit}"));
    Ok((sql, params))
}
```

- [ ] **Step 3: Update existing `compile_select` callers (the test file)**

In `src/services/query-api/tests/sql_compile.rs`, every existing `compile_select(...)` call now needs the `derived` arg. Add `&[]` as the second-to-last argument (before the limit) to EACH existing call. (The other caller, `read_object`, is updated in Task 5.)

- [ ] **Step 4: New compile unit tests**

Append to `src/services/query-api/tests/sql_compile.rs` (import `DerivedAggregate`, `DerivedSelect` from `query_api::sql`, and `Aggregation`, `LinkBacking`, `TableRef`, `RowFilter`, `CompareOp`, `ScalarValue` from `control_plane_core` as needed):

```rust
#[test]
fn derived_fk_count_compiles_to_a_correlated_subquery() {
    let derived = vec![DerivedSelect::Aggregate(DerivedAggregate {
        name: "orderCount".into(),
        agg: Aggregation::Count,
        backing: LinkBacking::ForeignKey {
            from_column: "id".into(),
            to_column: "customer_id".into(),
        },
        target_table: TableRef { schema: "main".into(), name: "orders".into() },
        target_filters: vec![],
    })];
    let (sql, params) = compile_select(
        &TableRef { schema: "main".into(), name: "customer".into() },
        &["id".to_string()],
        &[],
        &[],
        &[],
        &derived,
        100,
    )
    .unwrap();
    assert_eq!(
        sql,
        "SELECT \"id\", (SELECT COUNT(*) FROM \"main\".\"orders\" sub \
         WHERE sub.\"customer_id\" = o.\"id\") AS \"orderCount\" \
         FROM \"main\".\"customer\" o LIMIT 100"
    );
    assert!(params.is_empty());
}

#[test]
fn derived_jointable_sum_with_target_filter_orders_params_first() {
    // SUM over a join-table link, with a target row-filter; its param precedes a source
    // eq-filter param (SELECT clause precedes WHERE).
    let derived = vec![DerivedSelect::Aggregate(DerivedAggregate {
        name: "totalSpend".into(),
        agg: Aggregation::Sum("amount".into()),
        backing: LinkBacking::JoinTable {
            table: TableRef { schema: "main".into(), name: "customer_order".into() },
            from_key: "id".into(),
            from_column: "customer_id".into(),
            to_column: "order_id".into(),
            to_key: "id".into(),
        },
        target_table: TableRef { schema: "main".into(), name: "orders".into() },
        target_filters: vec![RowFilter::Compare {
            property: "status".into(),
            op: CompareOp::Eq,
            value: ScalarValue::Text("shipped".into()),
        }],
    })];
    let (sql, params) = compile_select(
        &TableRef { schema: "main".into(), name: "customer".into() },
        &["id".to_string()],
        &[],
        &[],
        &[("region".to_string(), SqlValue::Text("CA".into()))],
        &derived,
        100,
    )
    .unwrap();
    assert_eq!(
        sql,
        "SELECT \"id\", (SELECT COALESCE(SUM(sub.\"amount\"), 0) FROM \"main\".\"orders\" sub \
         JOIN \"main\".\"customer_order\" j ON j.\"order_id\" = sub.\"id\" \
         WHERE j.\"customer_id\" = o.\"id\" AND (sub.\"status\" = ?)) AS \"totalSpend\" \
         FROM \"main\".\"customer\" o WHERE (\"region\" = ?) LIMIT 100"
    );
    assert_eq!(params, vec![SqlValue::Text("shipped".into()), SqlValue::Text("CA".into())]);
}

#[test]
fn masked_derived_emits_marker_no_subquery_no_alias() {
    let derived = vec![DerivedSelect::Masked("orderCount".into())];
    let (sql, params) = compile_select(
        &TableRef { schema: "main".into(), name: "customer".into() },
        &["id".to_string()],
        &[],
        &[],
        &[],
        &derived,
        100,
    )
    .unwrap();
    // No aggregate -> no outer alias; the derived is a constant marker column.
    assert_eq!(
        sql,
        "SELECT \"id\", '***' AS \"orderCount\" FROM \"main\".\"customer\" LIMIT 100"
    );
    assert!(params.is_empty());
}
```

- [ ] **Step 5: Run + lint + commit**

Run: `buck2 test //src/services/query-api:sql-compile > /tmp/t4.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t4.log` → all pass (existing + 3 new).
Run: `buck2 build '//src/services/query-api:query-api[clippy.txt]' 2>&1 | tail -5` → empty.

```bash
git add src/services/query-api/src/sql.rs src/services/query-api/tests/sql_compile.rs
git commit -m "feat(query-api): compile_select derived aggregate-over-link subqueries"
```

---

## Task 5: `read_object` integration + both-ends governance

Resolve and govern each derived property, append to the projection.

**Files:**
- Modify: `src/services/query-api/src/handler.rs`

- [ ] **Step 1: Add an agg-column helper near `project_allowed`**

```rust
/// The target column a derived aggregate reads, if any (COUNT reads none).
fn agg_column(a: &control_plane_core::Aggregation) -> Option<&str> {
    use control_plane_core::Aggregation::*;
    match a {
        Count => None,
        Sum(c) | Avg(c) | Min(c) | Max(c) => Some(c.as_str()),
    }
}
```

- [ ] **Step 2: Build the derived list in `read_object`**

In `read_object`, AFTER the `mask_cols` computation and the `eq_filters` validation (i.e. after the existing block ending at the `BadFilter` loop), and BEFORE the `compile_select` call, insert:

```rust
    // Derived properties (aggregate-over-link), governed both-ends. Resolved + appended
    // after the physical projection, in declaration order; omitted (like a denied column)
    // when the subject can't read the linked type, the link/target is missing, or the
    // aggregated column is denied on the target — never an error, just absent.
    let mut derived_names: Vec<String> = Vec::new();
    let mut derived_types: Vec<String> = Vec::new();
    let mut derived_selects: Vec<crate::sql::DerivedSelect> = Vec::new();
    // Only touch the link catalog when the type actually declares derived properties.
    if !object_type.derived.is_empty() {
        let links = deps.ontology.links(&type_name, PageReq::unbounded()).await?;
        for d in &object_type.derived {
            if denied.contains(&d.name) {
                continue;
            }
            if masked.contains(&d.name) {
                derived_names.push(d.name.clone());
                derived_types.push(d.ty.clone());
                derived_selects.push(crate::sql::DerivedSelect::Masked(d.name.clone()));
                continue;
            }
            let Some(link) = links.items.iter().find(|l| l.name == d.link) else {
                continue; // missing link -> omit (no define-time validation in part-1)
            };
            let target_pt = PolicyTarget::Type(link.to.clone());
            // Both-ends: the subject must be permitted to read the linked type.
            if deps.acl.check(&subject.0, Action::Read, &target_pt).await? == Decision::Deny {
                continue;
            }
            let target_type = match deps.ontology.get_type(&link.to).await {
                Ok(t) => t,
                Err(ControlPlaneError::NotFound(_)) => continue, // target type gone -> omit
                Err(other) => return Err(QueryError::ControlPlane(other)),
            };
            let (t_filters, t_denied, _t_masked) =
                load_policy(deps.acl, &subject.0, &target_pt).await?;
            // Don't leak a target column the subject may not see, via an aggregate over it.
            if let Some(col) = agg_column(&d.agg) {
                if t_denied.contains(col) {
                    continue;
                }
            }
            derived_names.push(d.name.clone());
            derived_types.push(d.ty.clone());
            derived_selects.push(crate::sql::DerivedSelect::Aggregate(crate::sql::DerivedAggregate {
                name: d.name.clone(),
                agg: d.agg.clone(),
                backing: link.backing.clone(),
                target_table: target_type.table.clone(),
                target_filters: t_filters,
            }));
        }
    }
```

- [ ] **Step 3: Pass `derived_selects` to `compile_select` and extend the output**

Change the `compile_select` call to pass `&derived_selects` before `DEFAULT_LIMIT`:

```rust
    let (sql, params) = compile_select(
        &object_type.table,
        &allowed,
        &mask_cols,
        &row_filters,
        &q.eq_filters,
        &derived_selects,
        DEFAULT_LIMIT,
    )?;
```

Replace the output construction (the `logical_types`/`debug_assert_eq!`/`Ok(ObjectRows {...})` block) with one that appends the derived columns:

```rust
    let served = deps.serving.fetch_rows(&sql, &params).await?;
    // Output columns = physical `allowed` (in order) ++ surviving derived (in order).
    let mut columns = allowed.clone();
    columns.extend(derived_names.iter().cloned());
    // Logical types: physical from the type's properties; derived from their declared ty.
    let mut logical_types: Vec<String> = allowed
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
    logical_types.extend(derived_types.iter().cloned());
    // compile_select SELECTs physical `allowed` then derived names, in order, so the
    // serving engine must echo exactly that column order — the contract that lets us zip
    // logical_types/columns onto each row's cells by position.
    debug_assert_eq!(
        served.columns, columns,
        "serving engine returned columns out of the projected order"
    );
    Ok(ObjectRows {
        columns,
        logical_types,
        rows: served.rows,
    })
```

(`PolicyTarget`, `Action`, `Decision`, `ControlPlaneError`, `PageReq` are already imported in `handler.rs`; `load_policy` is in-module. Confirm `q.eq_filters` is still validated against the PHYSICAL `allowed`/`masked` only — derived names are not valid eq-filter targets, which the existing `BadFilter` loop already enforces since they aren't in `allowed`.)

- [ ] **Step 4: Build + lint + commit**

Run: `buck2 build //src/services/query-api:query-api 2>&1 | tail -10` (clean).
Run: `buck2 build '//src/services/query-api:query-api[clippy.txt]' 2>&1 | tail -5` (empty).
Run the existing read tests to confirm no regression: `buck2 test //src/services/query-api:governed-read //src/services/query-api:bind-read-e2e > /tmp/t5.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t5.log` (pass — no derived defined, behavior unchanged).

```bash
git add src/services/query-api/src/handler.rs
git commit -m "feat(query-api): read_object serves governed derived properties"
```

---

## Task 6: Governed derived-properties e2e

**Files:**
- Create: `src/services/query-api/tests/derived_properties_e2e.rs`
- Modify: `src/services/query-api/BUCK`

- [ ] **Step 1: Write the e2e**

Mirror `tests/link_traversal.rs`/`bind_read_e2e.rs` for the fixture + ACL setup. Land `customer` + `orders`, define the types (Customer with a derived `orderCount`=COUNT and `totalSpend`=SUM(amount) over an FK link `orders`), define the FK link, grant Read on both types, and assert the derived values; then prove both-ends governance.

```rust
//! Derived properties e2e: Customer.orderCount (COUNT) + totalSpend (SUM) over the
//! Customer->Order FK link, served through read_object. Both-ends governance: without
//! Read on Order the derived props are omitted; an Order row-filter narrows the aggregate.

use control_plane_core::{
    Acl, Action, Aggregation, Cardinality, CompareOp, DerivedPropertyDef, Effect, LinkBacking,
    LinkDef, ObjectType, Ontology, PropertyDef, PolicyTarget, Policy, RoleId, RowFilter,
    ScalarValue, SubjectId, TableRef, TypeName,
};
use control_plane_postgres::fixture::{DuckLakeWriter, PgFixture};
use query_api::handler::{ObjectQuery, QueryDeps, Subject, read_object};
use query_api::render::objects_to_json;
use query_api::serving::EmbeddedDuckDb;
use serde_json::json;

// NOTE: confirm the exact fixture seeding API against tests/link_traversal.rs — it lands
// customer + orders rows (via DuckLakeWriter::seed or the ingest materializer). Use the
// SAME approach that file uses; the assertions below are the contract.

#[tokio::test(flavor = "multi_thread")]
async fn derived_aggregates_are_served_and_governed() {
    // ---- fixture: customer(id) ; orders(id, customer_id, amount, status) ----
    // Customer 1 has orders (amount 5.0 'shipped', 7.0 'pending'); customer 2 has none.
    // (Seed via the same mechanism tests/link_traversal.rs uses.)
    // ... fixture setup (mirror link_traversal.rs) producing `cp`, `db`, `data_path` ...

    // ---- ontology: types + FK link + derived properties ----
    let customer = TypeName("Customer".into());
    let order = TypeName("Order".into());
    cp.ontology().define_type(ObjectType {
        name: order.clone(),
        properties: vec![
            PropertyDef { name: "id".into(), ty: "Long".into(), required: true },
            PropertyDef { name: "customer_id".into(), ty: "Long".into(), required: true },
            PropertyDef { name: "amount".into(), ty: "Double".into(), required: false },
            PropertyDef { name: "status".into(), ty: "String".into(), required: false },
        ],
        derived: vec![],
        table: TableRef { schema: "main".into(), name: "orders".into() },
    }).await.unwrap();
    cp.ontology().define_type(ObjectType {
        name: customer.clone(),
        properties: vec![PropertyDef { name: "id".into(), ty: "Long".into(), required: true }],
        derived: vec![
            DerivedPropertyDef { name: "orderCount".into(), ty: "Long".into(), link: "orders".into(), agg: Aggregation::Count },
            DerivedPropertyDef { name: "totalSpend".into(), ty: "Double".into(), link: "orders".into(), agg: Aggregation::Sum("amount".into()) },
        ],
        table: TableRef { schema: "main".into(), name: "customer".into() },
    }).await.unwrap();
    cp.ontology().define_link(LinkDef {
        name: "orders".into(),
        from: customer.clone(),
        to: order.clone(),
        cardinality: Cardinality::Many,
        backing: LinkBacking::ForeignKey { from_column: "id".into(), to_column: "customer_id".into() },
    }).await.unwrap();

    // ---- subject A: Read on Customer AND Order -> sees derived ----
    let a = SubjectId("a".into());
    let role_a = RoleId("ra".into());
    cp.define_subject(&a).await.unwrap();
    cp.define_role(&role_a).await.unwrap();
    cp.assign_role(&a, &role_a).await.unwrap();
    cp.grant(&role_a, Action::Read, PolicyTarget::Type(customer.clone()), Effect::Allow).await.unwrap();
    cp.grant(&role_a, Action::Read, PolicyTarget::Type(order.clone()), Effect::Allow).await.unwrap();

    let eng = EmbeddedDuckDb::attach(
        &format!("dbname={db} host={} user=postgres", fx.socket_path().display()),
        data_path,
    ).await.unwrap();
    let deps = QueryDeps { ontology: &cp, acl: &cp, serving: &eng };

    let rows = read_object(
        &ObjectQuery { type_name: "Customer".into(), eq_filters: vec![] },
        &Subject(a.clone()),
        &deps,
    ).await.unwrap();
    let body = objects_to_json(&rows);
    let mut objs: Vec<serde_json::Value> = body["objects"].as_array().unwrap().clone();
    objs.sort_by_key(|o| o["id"].as_str().unwrap().to_string());
    // Customer 1: 2 orders, total 12.0 ; Customer 2: 0 orders, total 0.0.
    assert_eq!(objs[0], json!({ "id": "1", "orderCount": "2", "totalSpend": 12.0 }));
    assert_eq!(objs[1], json!({ "id": "2", "orderCount": "0", "totalSpend": 0.0 }));

    // ---- subject B: Read on Customer ONLY -> derived OMITTED (both-ends) ----
    let b = SubjectId("b".into());
    let role_b = RoleId("rb".into());
    cp.define_subject(&b).await.unwrap();
    cp.define_role(&role_b).await.unwrap();
    cp.assign_role(&b, &role_b).await.unwrap();
    cp.grant(&role_b, Action::Read, PolicyTarget::Type(customer.clone()), Effect::Allow).await.unwrap();
    let rows_b = read_object(
        &ObjectQuery { type_name: "Customer".into(), eq_filters: vec![] },
        &Subject(b.clone()),
        &deps,
    ).await.unwrap();
    assert_eq!(rows_b.columns, vec!["id".to_string()], "no Read on Order -> derived omitted");

    // ---- subject C: Read on both, but an Order row-filter (status='shipped') ----
    let c = SubjectId("c".into());
    let role_c = RoleId("rc".into());
    cp.define_subject(&c).await.unwrap();
    cp.define_role(&role_c).await.unwrap();
    cp.assign_role(&c, &role_c).await.unwrap();
    cp.grant(&role_c, Action::Read, PolicyTarget::Type(customer.clone()), Effect::Allow).await.unwrap();
    cp.grant(&role_c, Action::Read, PolicyTarget::Type(order.clone()), Effect::Allow).await.unwrap();
    cp.set_policy(&role_c, Policy {
        target: PolicyTarget::Type(order.clone()),
        row_filter: Some(RowFilter::Compare { property: "status".into(), op: CompareOp::Eq, value: ScalarValue::Text("shipped".into()) }),
        deny_columns: vec![],
        mask_columns: vec![],
    }).await.unwrap();
    let rows_c = read_object(
        &ObjectQuery { type_name: "Customer".into(), eq_filters: vec![] },
        &Subject(c.clone()),
        &deps,
    ).await.unwrap();
    let body_c = objects_to_json(&rows_c);
    let mut objs_c: Vec<serde_json::Value> = body_c["objects"].as_array().unwrap().clone();
    objs_c.sort_by_key(|o| o["id"].as_str().unwrap().to_string());
    // Only the 'shipped' order (amount 5.0) counts for customer 1.
    assert_eq!(objs_c[0], json!({ "id": "1", "orderCount": "1", "totalSpend": 5.0 }));
}
```

**IMPORTANT — fill the fixture seeding** by mirroring `src/services/query-api/tests/link_traversal.rs` EXACTLY (the same `PgFixture`/`DuckLakeWriter` setup, the same way it lands `customer` + `orders` rows, and how it builds `data_path`/`fx`/`cp`/`db`). Read that file first; reproduce its setup verbatim, then layer the ontology + ACL + assertions above. The seeded data must be: `customer` rows id=1,2; `orders` rows (id, customer_id, amount, status): (10,1,5.0,'shipped'), (11,1,7.0,'pending'). Also confirm `set_policy`/`Policy` is the real ACL API for a row-filter (mirror `governed_read.rs`); if the API differs, align to it. Do not weaken the three assertions (served+correct, both-ends-omit, target-filter-narrows).

- [ ] **Step 2: BUCK target**

```python
loom_fixture_test(
    name = "derived-properties-e2e",
    crate = "derived_properties_e2e",
    srcs = ["tests/derived_properties_e2e.rs"],
    crate_root = "tests/derived_properties_e2e.rs",
    duckdb = True,
    deps = [
        ":query-api",
        "//src/control-plane/core:core",
        "//src/control-plane/postgres:postgres",
        "//third-party:serde_json",
        "//third-party:tokio",
    ],
)
```

- [ ] **Step 3: Run + commit**

Run: `buck2 test //src/services/query-api:derived-properties-e2e > /tmp/t6.log 2>&1; grep -E "Tests finished|FAIL|panicked|assertion" /tmp/t6.log` → `Pass 1. Fail 0.`
If the aggregate values are wrong, check the FK correlation direction (`sub.customer_id = o.id`) and the seeded data. Do not weaken assertions.

```bash
git add src/services/query-api/tests/derived_properties_e2e.rs src/services/query-api/BUCK
git commit -m "test(query-api): derived-properties e2e — served + both-ends governed"
```

---

## Task 7: Docs

**Files:**
- Modify: `docs/superpowers/specs/2026-06-06-loom-roadmap.md`, `docs/FUTURE.md`

- [ ] **Step 1: Roadmap delivered marker**

Find the Query richer-reads entries (link traversal is marked delivered). Add a sibling **derived properties (slice B)** delivered entry matching the adjacent format: aggregate-over-link derived properties (`COUNT`/`SUM`/`AVG`/`MIN`/`MAX`) served through `read_object` as governed correlated subqueries, both-ends governed, proven by an e2e. Reference `docs/superpowers/specs/2026-06-15-derived-properties-design.md`. Update the FUTURE.md "remaining relational-read slices" note that called this slice B (mark B done; C multi-hop remains).

- [ ] **Step 2: FUTURE.md follow-ups**

In `docs/FUTURE.md` (match the existing style), add the derived-properties follow-ups: scalar/expression derived properties; derived properties on traversal output (`read_linked_objects`); multi-hop aggregates / derived-on-derived; materialization; define-time validation of a derived property's link/target/column/type; derived props as filter/sort targets.

- [ ] **Step 3: Commit**

```bash
git add docs/superpowers/specs/2026-06-06-loom-roadmap.md docs/FUTURE.md
git commit -m "docs: derived properties (slice B) delivered; record follow-ups"
```

---

## Final Verification

- [ ] `buck2 build //src/... 2>&1 | tail -20` — clean.
- [ ] `buck2 test //src/... > /tmp/sweep.log 2>&1; grep -E "Tests finished|FAIL" /tmp/sweep.log` — `Fail 0`, including `sql-compile`, `derived-properties-e2e`, the ontology contracts, and the existing read tests.
- [ ] `tools/clippy-all.sh 2>&1 | tail -5` — clean.
- [ ] `git status` — the only `.sqlx` change is the new derived-property queries (Task 2). No `Cargo.lock`/`third-party` drift.
- [ ] **Run buck2 commands serially** — never a second buck2 invocation (or a commit whose hooks run buck2) while a `buck2 test //src/...` sweep runs.

Then proceed to **superpowers:finishing-a-development-branch**.
