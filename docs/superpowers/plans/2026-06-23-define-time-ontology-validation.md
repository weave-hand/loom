# Define-time ontology validation — Implementation Plan

> **For agentic workers:** implement this plan task-by-task under TDD (write the
> failing test first, then the code). Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reject — at authoring time, with a collect-all-violations error naming
each bad reference — a `define_type` whose derived properties reference a missing
link/target/column or an inapplicable aggregation, and a `define_link` whose
backing columns do not exist in the endpoint/join tables. Validation reads the
catalog through the existing `bind` seam; the `Ontology` trait stays decoupled
from the catalog. No behavior change for already-defined, valid ontologies.
Closes `road-define-time-ontology-validation`.

**Spec:** `docs/superpowers/specs/2026-06-23-define-time-ontology-validation-design.md`

**Architecture:** Both validators live in the `ingest` crate alongside the
existing `bind` (`src/services/ingest/src/bind.rs`), which already takes
`&dyn Catalog + &dyn Ontology` and is the define-time conformance seam.

- **Derived-property validation extends `bind`**: a new pass over
  `type_def.derived` (collect-all, alongside the existing property/identity/
  reserved-name passes) that resolves each derived property's link, the link's
  target table, and the aggregation column/result type.
- **Link validation is a sibling `bind_link`**: same module + shape as `bind`,
  validating a `LinkDef`'s backing columns against the catalog, then persisting
  via `define_link` only when clean. `define_link` already checks endpoint
  *types* exist; `bind_link` adds the physical-column gate.

No control-plane / ontology / ACL trait changes, **no new SQL** — the validators
use only existing `Catalog::current_snapshot`/`Catalog::schema` and `Ontology`
reads, so no `.sqlx` cache change is expected.

**Tech Stack:** Rust, buck2. Tests are `rust_test` integration targets (no inline
`#[cfg(test)]`). The new unit tests are pure (non-fixture) `rust_test`s over
`MemoryControlPlane` (which implements both `Catalog` and `Ontology`); the
existing fixture-backed `bind` target gains the Postgres-parity cases.

## Global Constraints

- **Tests are `rust_test` integration targets only** — NO inline `#[cfg(test)]` /
  `#[test]` in `src/**.rs` (the `no-inline-tests` prek hook fails).
- **No new third-party deps** — `control-plane-memory`, `core`, `tokio` are all
  already wired into ingest test targets (see the `materialize`/`http-land`
  targets). No `Cargo.toml` / `Cargo.lock` / `third-party/BUCK` change; the
  `duckdb`-downgrade footgun does not apply.
- **No regression for valid, already-defined ontologies** — the derived pass only
  runs when `type_def.derived` is non-empty; `bind_link` is a new function with no
  existing caller, so existing `bind` behavior for derived-free types is unchanged.
- **JoinTable semantics (CORRECTION to the spec prose).** The spec's link-validation
  prose says "`from_column` exists on the from-type's table, `to_column` on the
  to-type's table, and `from_key`/`to_key` exist on the join table." That is
  **backwards** relative to the actual runtime join the traversal compiler emits
  (`src/services/query-api/src/sql.rs:588-604`) and the `LinkBacking` doc-comment
  (`core/src/ontology.rs:60-61`): `from_table.from_key = join.from_column AND
  join.to_column = to_table.to_key`. Implement the **codebase-consistent** mapping
  — `from_key`→from-type's table, `to_key`→to-type's table, `from_column` &
  `to_column`→the join table — so define-time validation matches how links are
  actually traversed. (Validating against the spec's swapped prose would reject
  valid links and accept broken ones.)
- **`ontology.links()` of the not-yet-defined type.** `bind` persists `type_def`
  via `define_type` only at the very end, so when the derived pass calls
  `ontology.links(&type_def.name)` the type may not exist yet → the memory and
  Postgres `links` impls return `NotFound`. Treat `NotFound` as "no links" (empty
  list) — every derived link is then `UnknownDerivedLink`, correctly enforcing the
  authoring order (define types → define links → bind type with derived). Any
  other error propagates as `BindError::ControlPlane`.
- Commit messages: Conventional Commits.
- Run tests with the file-redirect pattern (never pipe `buck2 test` through
  `tail`/`head`):
  `buck2 test //src/services/ingest/... > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t.log`.

---

### Task 1: New `BindViolationReason` variants + classification helpers + derived-property validation in `bind`

**Files:**
- Modify: `src/services/ingest/src/bind.rs` — add 4 `BindViolationReason` variants,
  the agg/result-type classification helpers, and the derived-property pass in `bind`.
- Create: `src/services/ingest/tests/bind_memory.rs` — pure memory-fake tests
  (derived cases in this task; `bind_link` cases added in Task 2 to the same file).
- Modify: `src/services/ingest/BUCK` — add the `bind-memory` rust_test target.

**Interfaces produced (used by Task 2 / tests):**
- New `BindViolationReason` variants:
  `UnknownDerivedLink(String)`, `MissingAggColumn`,
  `BadAggType { agg: String, column: String }`,
  `BadDerivedResultType { declared: String, expected: String }`.

- [ ] **Step 1: Write the failing derived-validation tests**

Create `src/services/ingest/tests/bind_memory.rs`. `MemoryControlPlane` implements
both `Catalog` and `Ontology`; `seed_catalog(table, &[(name, logical_ty, nullable)], &[batches])`
creates a live table with those columns. Column `ty` strings here are loom **logical**
type names (`"long"`, `"double"`, `"string"`, `"boolean"`), matching what the catalog
returns. Authoring order in each test: seed catalog tables → `define_type` source +
target (no derived) → `define_link` → `bind` source WITH the derived property.

```rust
//! Define-time validation against the in-memory fakes: derived-property and
//! bind_link validation collect all violations and persist nothing on rejection.

use std::time::Duration;

use control_plane_core::{
    Aggregation, Cardinality, ControlPlaneError, DerivedPropertyDef, LinkBacking, LinkDef,
    ObjectType, Ontology, PropertyDef, TableRef, TypeName,
};
use control_plane_memory::MemoryControlPlane;
use ingest::{BindError, BindViolationReason, bind, bind_link};

fn cp() -> MemoryControlPlane {
    MemoryControlPlane::new(Duration::from_millis(300))
}
fn tref(name: &str) -> TableRef {
    TableRef { schema: "main".into(), name: name.into() }
}
fn prop(name: &str, ty: &str, required: bool) -> PropertyDef {
    PropertyDef { name: name.into(), ty: ty.into(), required }
}
fn derived(name: &str, ty: &str, link: &str, agg: Aggregation) -> DerivedPropertyDef {
    DerivedPropertyDef { name: name.into(), ty: ty.into(), link: link.into(), agg }
}

/// Seed an Order (table `order`, FK `customer_id`) source type and a Customer
/// (table `customer`, cols id/amount/name/flag) target type, linked Order->Customer
/// via FK `customer_id = id`. Returns the cp ready to `bind` an Order WITH derived.
async fn seed_order_customer() -> MemoryControlPlane {
    let cp = cp();
    cp.seed_catalog(&tref("order"), &[("id".into(), "long".into(), false),
        ("customer_id".into(), "long".into(), true)], &[1]);
    cp.seed_catalog(&tref("customer"), &[
        ("id".into(), "long".into(), false),
        ("amount".into(), "double".into(), true),
        ("name".into(), "string".into(), true),
        ("flag".into(), "boolean".into(), true)], &[1]);
    cp.define_type(ObjectType {
        name: TypeName("Order".into()),
        properties: vec![prop("id", "Long", true), prop("customer_id", "Long", false)],
        derived: vec![],
        table: tref("order"),
        identity: None,
    }).await.unwrap();
    cp.define_type(ObjectType {
        name: TypeName("Customer".into()),
        properties: vec![prop("id", "Long", true)],
        derived: vec![],
        table: tref("customer"),
        identity: None,
    }).await.unwrap();
    cp.define_link(LinkDef {
        name: "customer".into(),
        from: TypeName("Order".into()),
        to: TypeName("Customer".into()),
        cardinality: Cardinality::One,
        backing: LinkBacking::ForeignKey {
            from_column: "customer_id".into(),
            to_column: "id".into(),
        },
    }).await.unwrap();
    cp
}

/// Re-bind Order with one derived property; return the collected violations.
async fn bind_order_derived(cp: &MemoryControlPlane, d: DerivedPropertyDef) -> BindError {
    let ty = ObjectType {
        name: TypeName("Order".into()),
        properties: vec![prop("id", "Long", true), prop("customer_id", "Long", false)],
        derived: vec![d],
        table: tref("order"),
        identity: None,
    };
    bind(&cp, &cp, ty).await.unwrap_err()
}

#[tokio::test]
async fn valid_derived_properties_round_trip() {
    let cp = seed_order_customer().await;
    let ty = ObjectType {
        name: TypeName("Order".into()),
        properties: vec![prop("id", "Long", true), prop("customer_id", "Long", false)],
        derived: vec![
            derived("ct", "Long", "customer", Aggregation::Count),
            derived("total", "Double", "customer", Aggregation::Sum("amount".into())),
            derived("hi", "String", "customer", Aggregation::Max("name".into())),
        ],
        table: tref("order"),
        identity: None,
    };
    bind(&cp, &cp, ty.clone()).await.unwrap();
    assert_eq!(cp.get_type(&TypeName("Order".into())).await.unwrap(), ty);
}

#[tokio::test]
async fn unknown_derived_link_is_a_violation() {
    let cp = seed_order_customer().await;
    let err = bind_order_derived(&cp, derived("x", "Long", "nope", Aggregation::Count)).await;
    let BindError::DoesNotConform(v) = err else { panic!("{err:?}") };
    assert!(v.iter().any(|x| x.property == "x"
        && matches!(&x.reason, BindViolationReason::UnknownDerivedLink(l) if l == "nope")));
}

#[tokio::test]
async fn missing_agg_column_is_a_violation() {
    let cp = seed_order_customer().await;
    let err = bind_order_derived(&cp, derived("s", "Double", "customer", Aggregation::Sum("ghost".into()))).await;
    let BindError::DoesNotConform(v) = err else { panic!("{err:?}") };
    assert!(v.iter().any(|x| x.property == "s"
        && matches!(x.reason, BindViolationReason::MissingAggColumn)));
}

#[tokio::test]
async fn bad_agg_type_is_a_violation() {
    let cp = seed_order_customer().await;
    // Sum over a string column is not applicable.
    let err = bind_order_derived(&cp, derived("s", "Double", "customer", Aggregation::Sum("name".into()))).await;
    let BindError::DoesNotConform(v) = err else { panic!("{err:?}") };
    assert!(v.iter().any(|x| x.property == "s"
        && matches!(x.reason, BindViolationReason::BadAggType { .. })));
}

#[tokio::test]
async fn bad_derived_result_type_is_a_violation() {
    let cp = seed_order_customer().await;
    // Count must be an integer-category result; declaring String is inconsistent.
    let err = bind_order_derived(&cp, derived("c", "String", "customer", Aggregation::Count)).await;
    let BindError::DoesNotConform(v) = err else { panic!("{err:?}") };
    assert!(v.iter().any(|x| x.property == "c"
        && matches!(x.reason, BindViolationReason::BadDerivedResultType { .. })));
}

#[tokio::test]
async fn min_over_unordered_boolean_is_bad_agg_type() {
    let cp = seed_order_customer().await;
    let err = bind_order_derived(&cp, derived("m", "Boolean", "customer", Aggregation::Min("flag".into()))).await;
    let BindError::DoesNotConform(v) = err else { panic!("{err:?}") };
    assert!(v.iter().any(|x| x.property == "m"
        && matches!(x.reason, BindViolationReason::BadAggType { .. })));
}

#[tokio::test]
async fn collects_all_derived_violations_and_persists_nothing() {
    let cp = seed_order_customer().await;
    let ty = ObjectType {
        name: TypeName("Order".into()),
        properties: vec![prop("id", "Long", true), prop("customer_id", "Long", false)],
        derived: vec![
            derived("a", "Long", "nope", Aggregation::Count),                 // UnknownDerivedLink
            derived("b", "Double", "customer", Aggregation::Sum("ghost".into())), // MissingAggColumn
            derived("c", "String", "customer", Aggregation::Count),           // BadDerivedResultType
        ],
        table: tref("order"),
        identity: None,
    };
    let err = bind(&cp, &cp, ty).await.unwrap_err();
    let BindError::DoesNotConform(v) = err else { panic!("{err:?}") };
    assert!(v.iter().any(|x| x.property == "a"
        && matches!(x.reason, BindViolationReason::UnknownDerivedLink(_))));
    assert!(v.iter().any(|x| x.property == "b"
        && matches!(x.reason, BindViolationReason::MissingAggColumn)));
    assert!(v.iter().any(|x| x.property == "c"
        && matches!(x.reason, BindViolationReason::BadDerivedResultType { .. })));
    // Re-binding Order failed, so the original (derived-free) Order is unchanged.
    assert!(cp.get_type(&TypeName("Order".into())).await.unwrap().derived.is_empty());
}

#[tokio::test]
async fn derived_link_on_undefined_type_is_unknown_link() {
    // The type being bound does not exist yet: ontology.links(NotFound) -> treated
    // as empty -> every derived link is unknown (enforces authoring order).
    let cp = cp();
    cp.seed_catalog(&tref("ghost"), &[("id".into(), "long".into(), false)], &[1]);
    let ty = ObjectType {
        name: TypeName("Ghost".into()),
        properties: vec![prop("id", "Long", true)],
        derived: vec![derived("d", "Long", "any", Aggregation::Count)],
        table: tref("ghost"),
        identity: None,
    };
    let err = bind(&cp, &cp, ty).await.unwrap_err();
    let BindError::DoesNotConform(v) = err else { panic!("{err:?}") };
    assert!(v.iter().any(|x| x.property == "d"
        && matches!(x.reason, BindViolationReason::UnknownDerivedLink(_))));
}
```

Add to `src/services/ingest/BUCK`:

```python
rust_test(
    name = "bind-memory",
    crate = "bind_memory",
    srcs = ["tests/bind_memory.rs"],
    crate_root = "tests/bind_memory.rs",
    edition = "2024",
    deps = [
        ":ingest",
        "//src/control-plane/core:core",
        "//src/control-plane/memory:memory",
        "//third-party:tokio",
    ],
)
```

- [ ] **Step 2: Add the new `BindViolationReason` variants**

In `src/services/ingest/src/bind.rs`, extend the enum (keep existing variants):

```rust
    /// A derived property names a link not defined on this type.
    UnknownDerivedLink(String),
    /// A derived property's aggregation column is absent from the target table.
    MissingAggColumn,
    /// The aggregation is not applicable to the target column's type (e.g. Sum over
    /// a non-numeric column, Min/Max over an unordered column). `agg` names the
    /// aggregation, `column` the offending target column.
    BadAggType { agg: String, column: String },
    /// The derived property's declared result type is unknown, or inconsistent with
    /// the aggregation's result category (Count -> integer, Sum/Avg -> numeric,
    /// Min/Max -> the column's type). `declared` is the declared logical type,
    /// `expected` the required category/type.
    BadDerivedResultType { declared: String, expected: String },
```

- [ ] **Step 3: Add the classification + derived-validation helpers**

In `bind.rs`, add private helpers (use `control_plane_core::{BaseType, resolve_logical}`;
add them to the existing `use control_plane_core::{...}` import — and `Aggregation`,
`Catalog`, `LinkDef`, `LinkBacking`, `TableSchema` as needed for this task and Task 2):

```rust
/// Sum/Avg require a numeric column.
fn is_numeric(b: BaseType) -> bool {
    matches!(b, BaseType::Integer | BaseType::Long | BaseType::Double)
}
/// Min/Max require an ordered column. Boolean is excluded (a degenerate min/max).
fn is_ordered(b: BaseType) -> bool {
    matches!(
        b,
        BaseType::Integer | BaseType::Long | BaseType::Double
            | BaseType::Date | BaseType::Timestamp | BaseType::String
    )
}
/// Human name of an aggregation, for the `BadAggType` violation.
fn agg_name(a: &Aggregation) -> &'static str {
    match a {
        Aggregation::Count => "Count",
        Aggregation::Sum(_) => "Sum",
        Aggregation::Avg(_) => "Avg",
        Aggregation::Min(_) => "Min",
        Aggregation::Max(_) => "Max",
    }
}
/// The column an aggregation reads, if any (Count reads none).
fn agg_column(a: &Aggregation) -> Option<&str> {
    match a {
        Aggregation::Count => None,
        Aggregation::Sum(c) | Aggregation::Avg(c) | Aggregation::Min(c) | Aggregation::Max(c) => {
            Some(c.as_str())
        }
    }
}
```

- [ ] **Step 4: Add the derived-property pass to `bind`**

In `bind`, after the existing property/identity loops and *before* the
`if !violations.is_empty()` check (so derived violations join the collect-all set),
add the pass. Keep the existing reserved-name derived loop. Resolve the source
type's links once (guarded on non-empty `derived`, with the `NotFound`→empty
handling from Global Constraints):

```rust
    // Derived-property validation: each derived property must name a link defined on
    // this type, and its aggregation must be applicable to the target column with a
    // consistent result type. Collected alongside the property violations above.
    if !type_def.derived.is_empty() {
        let links = match ontology.links(&type_def.name, PageReq::unbounded()).await {
            Ok(page) => page.items,
            // The type being bound is not persisted until the end of bind, so a
            // brand-new type has no links yet -> every derived link is unknown.
            Err(ControlPlaneError::NotFound(_)) => Vec::new(),
            Err(e) => return Err(BindError::ControlPlane(e)),
        };
        for d in &type_def.derived {
            let Some(link) = links.iter().find(|l| l.name == d.link) else {
                violations.push(BindViolation {
                    property: d.name.clone(),
                    reason: BindViolationReason::UnknownDerivedLink(d.link.clone()),
                });
                continue;
            };
            // The column's logical type from the target table (None for Count, or when
            // the column is absent). Only column-bearing aggs read the target schema.
            let col_base: Option<BaseType> = if let Some(col_name) = agg_column(&d.agg) {
                let target_table = ontology.resolve(&link.to).await?;
                let snap = catalog.current_snapshot(&target_table).await?;
                let schema = catalog.schema(&target_table, snap.id).await?;
                match schema.columns.iter().find(|c| c.name == col_name) {
                    None => {
                        violations.push(BindViolation {
                            property: d.name.clone(),
                            reason: BindViolationReason::MissingAggColumn,
                        });
                        None // column missing -> applicability + Min/Max result checks skipped
                    }
                    Some(col) => {
                        let cb = resolve_logical(&col.ty);
                        // Applicability: Sum/Avg numeric, Min/Max ordered.
                        let applicable = match (&d.agg, cb) {
                            (Aggregation::Sum(_) | Aggregation::Avg(_), Some(b)) => is_numeric(b),
                            (Aggregation::Min(_) | Aggregation::Max(_), Some(b)) => is_ordered(b),
                            (_, None) => false, // unknown column type -> not applicable
                            _ => true,
                        };
                        if !applicable {
                            violations.push(BindViolation {
                                property: d.name.clone(),
                                reason: BindViolationReason::BadAggType {
                                    agg: agg_name(&d.agg).into(),
                                    column: col_name.into(),
                                },
                            });
                        }
                        cb
                    }
                }
            } else {
                None
            };
            // Result-type consistency. Declared ty must be a known logical type and
            // match the agg's result category.
            let declared = resolve_logical(&d.ty);
            let (ok, expected): (bool, String) = match &d.agg {
                Aggregation::Count => (
                    matches!(declared, Some(BaseType::Integer | BaseType::Long)),
                    "integer".into(),
                ),
                Aggregation::Sum(_) | Aggregation::Avg(_) => (
                    declared.is_some_and(is_numeric),
                    "numeric".into(),
                ),
                Aggregation::Min(_) | Aggregation::Max(_) => match col_base {
                    // Min/Max preserve the column's type; if the column was missing we
                    // already reported MissingAggColumn and skip this check.
                    Some(cb) => (declared == Some(cb), cb.canonical_name().into()),
                    None => (true, String::new()),
                },
            };
            if !ok {
                violations.push(BindViolation {
                    property: d.name.clone(),
                    reason: BindViolationReason::BadDerivedResultType {
                        declared: d.ty.clone(),
                        expected,
                    },
                });
            }
        }
    }
```

> Note: `PageReq::unbounded()` is the unbounded page request the query handler uses
> (`handler.rs:251`); confirm it is exported from `control_plane_core` (it is — the
> handler imports it) and add `PageReq` to the `bind.rs` import.

- [ ] **Step 5: Run the derived tests**

```
buck2 test //src/services/ingest:bind-memory > /tmp/t.log 2>&1
grep -E "Tests finished|FAIL|error\[" /tmp/t.log
```

---

### Task 2: `bind_link` sibling validator (+ memory tests)

**Files:**
- Modify: `src/services/ingest/src/bind.rs` — add `pub async fn bind_link`.
- Modify: `src/services/ingest/src/lib.rs` — re-export `bind_link`.
- Modify: `src/services/ingest/tests/bind_memory.rs` — add the `bind_link` tests.

**Interfaces produced:**
- `pub async fn bind_link(catalog: &dyn Catalog, ontology: &dyn Ontology, link: LinkDef) -> Result<(), BindError>`.

- [ ] **Step 1: Write the failing `bind_link` tests**

Append to `tests/bind_memory.rs`. Seeds two endpoint types + (for JoinTable) a join
table; the **JoinTable** column→table mapping follows the codebase semantics
(`from_key`→from-type's table, `to_key`→to-type's table, `from_column`/`to_column`→
join table — see the Global Constraints correction).

```rust
async fn two_types() -> MemoryControlPlane {
    let cp = cp();
    // From-type Person(table person, cols id, employer_id, team_id)
    cp.seed_catalog(&tref("person"), &[("id".into(), "long".into(), false),
        ("employer_id".into(), "long".into(), true),
        ("team_id".into(), "long".into(), true)], &[1]);
    // To-type Company(table company, cols id)
    cp.seed_catalog(&tref("company"), &[("id".into(), "long".into(), false)], &[1]);
    cp.define_type(ObjectType { name: TypeName("Person".into()),
        properties: vec![prop("id", "Long", true)], derived: vec![],
        table: tref("person"), identity: None }).await.unwrap();
    cp.define_type(ObjectType { name: TypeName("Company".into()),
        properties: vec![prop("id", "Long", true)], derived: vec![],
        table: tref("company"), identity: None }).await.unwrap();
    cp
}

fn fk_link(from_column: &str, to_column: &str) -> LinkDef {
    LinkDef {
        name: "employer".into(),
        from: TypeName("Person".into()),
        to: TypeName("Company".into()),
        cardinality: Cardinality::One,
        backing: LinkBacking::ForeignKey {
            from_column: from_column.into(),
            to_column: to_column.into(),
        },
    }
}

#[tokio::test]
async fn bind_link_fk_good_round_trips() {
    let cp = two_types().await;
    bind_link(&cp, &cp, fk_link("employer_id", "id")).await.unwrap();
    let links = cp.links(&TypeName("Person".into()), control_plane_core::PageReq::unbounded())
        .await.unwrap();
    assert!(links.items.iter().any(|l| l.name == "employer"));
}

#[tokio::test]
async fn bind_link_fk_bad_from_column_is_missing_column() {
    let cp = two_types().await;
    let err = bind_link(&cp, &cp, fk_link("nope", "id")).await.unwrap_err();
    let BindError::DoesNotConform(v) = err else { panic!("{err:?}") };
    assert!(v.iter().any(|x| x.property == "nope"
        && matches!(x.reason, BindViolationReason::MissingColumn)));
    // Nothing persisted.
    assert!(cp.links(&TypeName("Person".into()), control_plane_core::PageReq::unbounded())
        .await.unwrap().items.is_empty());
}

#[tokio::test]
async fn bind_link_fk_bad_to_column_is_missing_column() {
    let cp = two_types().await;
    let err = bind_link(&cp, &cp, fk_link("employer_id", "nope")).await.unwrap_err();
    let BindError::DoesNotConform(v) = err else { panic!("{err:?}") };
    assert!(v.iter().any(|x| x.property == "nope"
        && matches!(x.reason, BindViolationReason::MissingColumn)));
}

/// JoinTable: membership(from_col person_id, to_col company_id); person.id, company.id.
fn jt_link(table: &str, from_key: &str, from_column: &str, to_column: &str, to_key: &str) -> LinkDef {
    LinkDef {
        name: "member".into(),
        from: TypeName("Person".into()),
        to: TypeName("Company".into()),
        cardinality: Cardinality::Many,
        backing: LinkBacking::JoinTable {
            table: tref(table),
            from_key: from_key.into(),
            from_column: from_column.into(),
            to_column: to_column.into(),
            to_key: to_key.into(),
        },
    }
}

async fn two_types_with_join() -> MemoryControlPlane {
    let cp = two_types().await;
    cp.seed_catalog(&tref("membership"), &[
        ("person_id".into(), "long".into(), false),
        ("company_id".into(), "long".into(), false)], &[1]);
    cp
}

#[tokio::test]
async fn bind_link_join_table_good_round_trips() {
    let cp = two_types_with_join().await;
    // from_key=person.id, from_column=membership.person_id,
    // to_column=membership.company_id, to_key=company.id  (codebase semantics)
    bind_link(&cp, &cp, jt_link("membership", "id", "person_id", "company_id", "id"))
        .await.unwrap();
    assert!(cp.links(&TypeName("Person".into()), control_plane_core::PageReq::unbounded())
        .await.unwrap().items.iter().any(|l| l.name == "member"));
}

#[tokio::test]
async fn bind_link_join_table_bad_from_key_on_from_table() {
    let cp = two_types_with_join().await;
    let err = bind_link(&cp, &cp, jt_link("membership", "nope", "person_id", "company_id", "id"))
        .await.unwrap_err();
    let BindError::DoesNotConform(v) = err else { panic!("{err:?}") };
    assert!(v.iter().any(|x| x.property == "nope"
        && matches!(x.reason, BindViolationReason::MissingColumn)));
}

#[tokio::test]
async fn bind_link_join_table_bad_join_column() {
    let cp = two_types_with_join().await;
    // from_column must exist on the JOIN table, not the from-type's table.
    let err = bind_link(&cp, &cp, jt_link("membership", "id", "nope", "company_id", "id"))
        .await.unwrap_err();
    let BindError::DoesNotConform(v) = err else { panic!("{err:?}") };
    assert!(v.iter().any(|x| x.property == "nope"
        && matches!(x.reason, BindViolationReason::MissingColumn)));
}

#[tokio::test]
async fn bind_link_join_table_absent_is_table_not_found() {
    let cp = two_types().await; // no membership table seeded
    let err = bind_link(&cp, &cp, jt_link("membership", "id", "person_id", "company_id", "id"))
        .await.unwrap_err();
    assert!(matches!(err, BindError::TableNotFound(t) if t.name == "membership"), "{err:?}");
}
```

- [ ] **Step 2: Implement `bind_link`**

In `src/services/ingest/src/bind.rs`, add (after `bind`). A small `schema_of`
helper maps a not-live table to `BindError::TableNotFound` (mirroring `bind`):

```rust
/// `table`'s schema at its current snapshot, mapping a not-live table to
/// `BindError::TableNotFound` (the same shape `bind` uses).
async fn schema_of(catalog: &dyn Catalog, table: &TableRef) -> Result<TableSchema, BindError> {
    let snap = match catalog.current_snapshot(table).await {
        Ok(s) => s,
        Err(ControlPlaneError::NotFound(_)) => return Err(BindError::TableNotFound(table.clone())),
        Err(e) => return Err(BindError::ControlPlane(e)),
    };
    Ok(catalog.schema(table, snap.id).await?)
}

/// Validate a link's backing columns against the catalog, then persist it via
/// `define_link` only when clean. `define_link` already checks both endpoint *types*
/// exist; `bind_link` adds the physical-column gate. Collects ALL missing-column
/// violations. Column→table mapping follows the runtime join the traversal compiler
/// emits (see the link-traversal design / `query-api::sql`):
///   ForeignKey: `from_column` on the from-type's table, `to_column` on the to-type's.
///   JoinTable:  `from_key` on the from-type's table, `to_key` on the to-type's table,
///               `from_column` and `to_column` on the join table (which must be live).
pub async fn bind_link(
    catalog: &dyn Catalog,
    ontology: &dyn Ontology,
    link: LinkDef,
) -> Result<(), BindError> {
    // Endpoint tables (a missing endpoint type surfaces as ControlPlane(NotFound)).
    let from_table = ontology.resolve(&link.from).await?;
    let to_table = ontology.resolve(&link.to).await?;
    let from_schema = schema_of(catalog, &from_table).await?;
    let to_schema = schema_of(catalog, &to_table).await?;

    let mut violations = Vec::new();
    let mut check = |schema: &TableSchema, col: &str| {
        if !schema.columns.iter().any(|c| c.name == col) {
            violations.push(BindViolation {
                property: col.to_string(),
                reason: BindViolationReason::MissingColumn,
            });
        }
    };
    match &link.backing {
        LinkBacking::ForeignKey { from_column, to_column } => {
            check(&from_schema, from_column);
            check(&to_schema, to_column);
        }
        LinkBacking::JoinTable { table, from_key, from_column, to_column, to_key } => {
            let join_schema = schema_of(catalog, table).await?;
            check(&from_schema, from_key);
            check(&to_schema, to_key);
            check(&join_schema, from_column);
            check(&join_schema, to_column);
        }
    }
    if !violations.is_empty() {
        return Err(BindError::DoesNotConform(violations));
    }
    ontology.define_link(link).await?;
    Ok(())
}
```

> Note: the `let mut check = |...|` closure borrows `violations` mutably; the
> `JoinTable` arm fetches `join_schema` (an `.await`) before the `check` calls, which
> is fine. If the borrow checker objects to the closure capturing across the `.await`,
> inline a small `fn missing(schema, col) -> bool` and push from the match arms instead.

- [ ] **Step 3: Re-export `bind_link`**

In `src/services/ingest/src/lib.rs`, extend the `pub use bind::{...}` line:

```rust
pub use bind::{BindError, BindViolation, BindViolationReason, bind, bind_link};
```

- [ ] **Step 4: Run the memory suite**

```
buck2 test //src/services/ingest:bind-memory > /tmp/t.log 2>&1
grep -E "Tests finished|FAIL|error\[" /tmp/t.log
```

---

### Task 3: Postgres parity cases (extend the existing fixture `bind` test)

**Files:**
- Modify: `src/services/ingest/tests/bind.rs` — add a derived-validation case and a
  `bind_link` case exercised through the hermetic Postgres + DuckLake fixture.

The existing `bind` target (`loom_fixture_test`, `duckdb = True`) already wires the
PG/DuckLake fixture. Add two tests proving the validators work against the real
adapters (not just memory). Keep them minimal — the memory suite covers the logic
matrix. Use `Aggregation::Count` for the derived parity case so it does not depend on
the DuckLake→logical column-type mapping (Count reads no column).

- [ ] **Step 1: Derived parity (valid + unknown-link)**

Extend the import line to include `bind_link` and `LinkBacking`, `LinkDef`,
`Cardinality` as needed. Add:

```rust
#[tokio::test]
async fn bind_validates_derived_against_real_catalog() {
    let fx = PgFixture::start();
    let (cp, db) = fx.fresh_db().await;
    let writer = DuckLakeWriter::new(fx.socket_path(), &db);
    seed_customer(&writer).await;
    // A second table to back the link target.
    writer.seed("main", "orders", &[("id".into(), "BIGINT".into(), false),
        ("customer_id".into(), "BIGINT".into(), true)], &[1]).await;

    // Define the two types and an Order->Customer link, then bind Order with a Count
    // derived over that link (valid) and over a missing link (UnknownDerivedLink).
    cp.define_type(ObjectType { name: TypeName("Customer".into()),
        properties: vec![prop("id", "Long", true)], derived: vec![],
        table: customer(), identity: None }).await.unwrap();
    cp.define_type(ObjectType { name: TypeName("Order".into()),
        properties: vec![prop("id", "Long", true)], derived: vec![],
        table: TableRef { schema: "main".into(), name: "orders".into() }, identity: None })
        .await.unwrap();
    cp.define_link(LinkDef { name: "customer".into(),
        from: TypeName("Order".into()), to: TypeName("Customer".into()),
        cardinality: Cardinality::One,
        backing: LinkBacking::ForeignKey { from_column: "customer_id".into(), to_column: "id".into() } })
        .await.unwrap();

    // Valid Count derived round-trips.
    let ok = ObjectType { name: TypeName("Order".into()),
        properties: vec![prop("id", "Long", true)],
        derived: vec![DerivedPropertyDef { name: "ct".into(), ty: "Long".into(),
            link: "customer".into(), agg: Aggregation::Count }],
        table: TableRef { schema: "main".into(), name: "orders".into() }, identity: None };
    bind(&cp, &cp, ok).await.unwrap();

    // Unknown link is rejected.
    let bad = ObjectType { name: TypeName("Order".into()),
        properties: vec![prop("id", "Long", true)],
        derived: vec![DerivedPropertyDef { name: "ct".into(), ty: "Long".into(),
            link: "nope".into(), agg: Aggregation::Count }],
        table: TableRef { schema: "main".into(), name: "orders".into() }, identity: None };
    let err = bind(&cp, &cp, bad).await.unwrap_err();
    assert!(matches!(err, BindError::DoesNotConform(ref v)
        if v.iter().any(|x| matches!(x.reason, BindViolationReason::UnknownDerivedLink(_)))), "{err:?}");
}
```

- [ ] **Step 2: `bind_link` parity (FK good + bad column)**

```rust
#[tokio::test]
async fn bind_link_validates_columns_against_real_catalog() {
    let fx = PgFixture::start();
    let (cp, db) = fx.fresh_db().await;
    let writer = DuckLakeWriter::new(fx.socket_path(), &db);
    seed_customer(&writer).await;
    writer.seed("main", "orders", &[("id".into(), "BIGINT".into(), false),
        ("customer_id".into(), "BIGINT".into(), true)], &[1]).await;
    cp.define_type(ObjectType { name: TypeName("Customer".into()),
        properties: vec![prop("id", "Long", true)], derived: vec![],
        table: customer(), identity: None }).await.unwrap();
    cp.define_type(ObjectType { name: TypeName("Order".into()),
        properties: vec![prop("id", "Long", true)], derived: vec![],
        table: TableRef { schema: "main".into(), name: "orders".into() }, identity: None })
        .await.unwrap();

    // Good FK: orders.customer_id -> customer.id
    bind_link(&cp, &cp, LinkDef { name: "customer".into(),
        from: TypeName("Order".into()), to: TypeName("Customer".into()),
        cardinality: Cardinality::One,
        backing: LinkBacking::ForeignKey { from_column: "customer_id".into(), to_column: "id".into() } })
        .await.unwrap();

    // Bad FK: a non-existent from-column.
    let err = bind_link(&cp, &cp, LinkDef { name: "bad".into(),
        from: TypeName("Order".into()), to: TypeName("Customer".into()),
        cardinality: Cardinality::One,
        backing: LinkBacking::ForeignKey { from_column: "ghost".into(), to_column: "id".into() } })
        .await.unwrap_err();
    assert!(matches!(err, BindError::DoesNotConform(ref v)
        if v.iter().any(|x| x.property == "ghost"
            && matches!(x.reason, BindViolationReason::MissingColumn))), "{err:?}");
}
```

> Confirm `DuckLakeWriter::seed` signature/behavior against `seed_customer` (it is
> `seed(schema, table, &[(name, ducklake_ty, nullable)], &[batches])`). The DuckLake
> type strings (`BIGINT`, `VARCHAR`) are mapped to logical on read by the adapter; the
> Count/`bind_link` parity cases above avoid depending on that mapping (Count reads no
> column; `bind_link` checks only column *existence*, not type).

- [ ] **Step 3: Run the fixture suite**

```
buck2 test //src/services/ingest:bind > /tmp/t.log 2>&1
grep -E "Tests finished|FAIL|error\[" /tmp/t.log
```

---

### Task 4: Full suite, clippy, lint, docs, commit/push

- [ ] **Step 1: Full `//src/...` suite**

```
buck2 test //src/... > /tmp/full.log 2>&1
grep -E "Tests finished|FAIL" /tmp/full.log
```

(The whole sweep guards against the shared-dep `duckdb`-downgrade regression
CLAUDE.md warns about, even though this diff adds no dep.)

- [ ] **Step 2: clippy + lint hooks**

```
./tools/clippy-all.sh > /tmp/clippy.log 2>&1; tail -5 /tmp/clippy.log
buck2 run //tools:prek -- run --all-files > /tmp/lint.log 2>&1; tail -20 /tmp/lint.log
```

Commit whatever the hooks rewrite (markdown EOF/whitespace, rustfmt).

- [ ] **Step 3: Confirm `.sqlx` is unchanged**

No new SQL is introduced. Confirm `git status` shows no change under
`src/control-plane/postgres/.sqlx/`. (If, unexpectedly, a query macro was touched,
run `tools/sqlx-prepare.sh` and commit — but none is expected.)

- [ ] **Step 4: Update `docs/ROADMAP.md`**

Mark `road-define-time-ontology-validation` done via `loom-docs-update`:
`- [ ]`→`- [x]`, `status:planned`→`status:done`, add `pr:#<n>` once the PR is open.
The spec folds `fut-define-time-chain-validation` (`status:dropped`); reflect that in
`docs/FUTURE.md` if it is still open.

- [ ] **Step 5: Commit and push**

```
git add -A
git commit -m "feat(ingest): define-time ontology validation (derived properties + bind_link)"
git push -u origin claude/blissful-euler-bhqqnt
```

> Branch note: this session is constrained to develop on `claude/blissful-euler-bhqqnt`
> (the skill's `work/<id>` convention is moot here — the cloud egress policy blocks the
> `refs/claim/*` mutex write, so there is no claim ref to bind a `work/<id>` branch to).
