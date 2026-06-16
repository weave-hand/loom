# Inverse-Direction Link Hops Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add inverse-direction link traversal (`target <--link-- source`), governed at every hop, across the single-hop and multi-hop governed read paths.

**Architecture:** The multi-hop SQL compiler (`query-api/src/sql.rs::compile_chain_with`) already joins symmetrically, so an inverse hop needs no compiler change — only (a) an inbound link lookup (`Ontology::links_to`) and (b) reversing the link backing's column roles (`LinkBacking::reversed`) before handing it to the compiler, following `link.from` instead of `link.to`. Direction is a per-hop property (`Hop { link, direction }`) parsed at the HTTP edge (`?direction=inverse` single-hop; `~`-prefixed path elements multi-hop) and resolved in `read_linked_chain`.

**Tech Stack:** Rust, buck2, async-trait, sqlx compile-time macros (postgres adapter), DuckDB serving engine, axum HTTP.

**Design spec:** `docs/superpowers/specs/2026-06-16-inverse-direction-hops-design.md`

**Build/test reminders (loom-specific):**
- Tests are integration `rust_test`/`loom_fixture_test` targets only — NEVER inline `#[cfg(test)]`/`#[test]` in `src/**` (the `no-inline-tests` prek hook fails the build).
- Run a target: `buck2 test //src/<path>:<target> > /tmp/t.log 2>&1; grep -nE "Tests finished|FAIL|error\[" /tmp/t.log`. NEVER pipe `buck2 test` through `tail`/`head` (it stalls).
- NEVER run two `buck2` invocations concurrently (single daemon).
- After changing any postgres SQL, regenerate the `.sqlx` cache: `./tools/sqlx-prepare.sh`, then commit the `.sqlx` change.
- Commit message trailer (every commit): `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>`. Conventional-commit subject required.

---

## File Structure

| File | Responsibility | Change |
|------|----------------|--------|
| `src/control-plane/core/src/ontology.rs` | ontology domain types + trait | add `LinkBacking::reversed()`; add `Ontology::links_to` trait method |
| `src/control-plane/core/tests/link_backing.rs` | NEW — `reversed()` unit tests | create |
| `src/control-plane/core/BUCK` | core test wiring | add `link-backing` target |
| `src/control-plane/memory/src/ontology.rs` | in-memory ontology | add `links_to` impl |
| `src/control-plane/postgres/src/ontology.rs` | postgres ontology | add `links_to` impl |
| `src/control-plane/postgres/.sqlx/` | committed sqlx cache | regenerate (new query) |
| `src/control-plane/testkit/src/lib.rs` | adapter contract suite | extend `ontology_contract` with inbound-links assertions |
| `src/services/query-api/src/handler.rs` | governed read core | `Direction`/`Hop` types, `ChainQuery.path: Vec<Hop>`, inverse resolution, `AmbiguousLink` |
| `src/services/query-api/src/path_parse.rs` | NEW — pure HTTP-edge direction parsing | create |
| `src/services/query-api/src/lib.rs` | crate module list | add `pub mod path_parse;` |
| `src/services/query-api/src/http.rs` | axum surface | parse `?direction=` + `~` sigils; map `AmbiguousLink` → 400 |
| `src/services/query-api/src/sql.rs` | SQL compiler | doc note only (direction-agnostic) |
| `src/services/query-api/tests/hop_types.rs` | NEW — `Hop`/`Direction` unit tests | create |
| `src/services/query-api/tests/path_parse.rs` | NEW — edge-parse unit tests | create |
| `src/services/query-api/tests/inverse_hops_e2e.rs` | NEW — fixture e2e | create |
| `src/services/query-api/BUCK` | query-api test wiring | add `hop-types`, `path-parse`, `inverse-hops-e2e` targets |

---

## Task 1: `LinkBacking::reversed()` in core

Pure column-role swap that turns a backing for `A --l--> B` into the backing the symmetric chain compiler needs to join `B` back to `A`. An involution.

**Files:**
- Modify: `src/control-plane/core/src/ontology.rs` (after the `LinkBacking` enum, ends line 66)
- Create: `src/control-plane/core/tests/link_backing.rs`
- Modify: `src/control-plane/core/BUCK`

- [ ] **Step 1: Write the failing test**

Create `src/control-plane/core/tests/link_backing.rs`:

```rust
//! `LinkBacking::reversed()` — the column-role swap that lets the symmetric chain
//! compiler traverse a link from its `to` side back to its `from` side.

use control_plane_core::{LinkBacking, TableRef};

#[test]
fn foreign_key_swaps_from_and_to_columns() {
    let fk = LinkBacking::ForeignKey {
        from_column: "id".into(),
        to_column: "customer_id".into(),
    };
    assert_eq!(
        fk.reversed(),
        LinkBacking::ForeignKey {
            from_column: "customer_id".into(),
            to_column: "id".into(),
        }
    );
}

#[test]
fn join_table_swaps_key_and_column_pairs_keeping_table() {
    let jt = LinkBacking::JoinTable {
        table: TableRef {
            schema: "main".into(),
            name: "customer_order".into(),
        },
        from_key: "id".into(),
        from_column: "customer_id".into(),
        to_column: "order_id".into(),
        to_key: "oid".into(),
    };
    assert_eq!(
        jt.reversed(),
        LinkBacking::JoinTable {
            table: TableRef {
                schema: "main".into(),
                name: "customer_order".into(),
            },
            from_key: "oid".into(),
            from_column: "order_id".into(),
            to_column: "customer_id".into(),
            to_key: "id".into(),
        }
    );
}

#[test]
fn reversed_is_an_involution() {
    let fk = LinkBacking::ForeignKey {
        from_column: "a".into(),
        to_column: "b".into(),
    };
    assert_eq!(fk.reversed().reversed(), fk);
    let jt = LinkBacking::JoinTable {
        table: TableRef {
            schema: "s".into(),
            name: "t".into(),
        },
        from_key: "fk".into(),
        from_column: "fc".into(),
        to_column: "tc".into(),
        to_key: "tk".into(),
    };
    assert_eq!(jt.reversed().reversed(), jt);
}
```

- [ ] **Step 2: Wire the test target**

In `src/control-plane/core/BUCK`, add after the `error-display` target (it ends at line 39, the `deps = [":core"]` block — mirror that minimal shape):

```python
rust_test(
    name = "link-backing",
    crate = "link_backing",
    srcs = ["tests/link_backing.rs"],
    crate_root = "tests/link_backing.rs",
    edition = "2024",
    visibility = ["PUBLIC"],
    deps = [":core"],
)
```

- [ ] **Step 3: Run the test to verify it fails**

Run: `buck2 test //src/control-plane/core:link-backing > /tmp/t.log 2>&1; grep -nE "Tests finished|FAIL|error\[|no method" /tmp/t.log`
Expected: FAIL — `no method named reversed found for enum LinkBacking`.

- [ ] **Step 4: Implement `reversed()`**

In `src/control-plane/core/src/ontology.rs`, immediately after the `LinkBacking` enum closing brace (line 66), add:

```rust
impl LinkBacking {
    /// The backing for traversing this link in the inverse direction (`to` -> `from`).
    /// The column roles are swapped so the same symmetric chain-join compiler reaches
    /// the origin type; the mapping table (for `JoinTable`) is unchanged. An involution:
    /// `b.reversed().reversed() == b`.
    pub fn reversed(&self) -> LinkBacking {
        match self {
            LinkBacking::ForeignKey {
                from_column,
                to_column,
            } => LinkBacking::ForeignKey {
                from_column: to_column.clone(),
                to_column: from_column.clone(),
            },
            LinkBacking::JoinTable {
                table,
                from_key,
                from_column,
                to_column,
                to_key,
            } => LinkBacking::JoinTable {
                table: table.clone(),
                from_key: to_key.clone(),
                from_column: to_column.clone(),
                to_column: from_column.clone(),
                to_key: from_key.clone(),
            },
        }
    }
}
```

- [ ] **Step 5: Run the test to verify it passes**

Run: `buck2 test //src/control-plane/core:link-backing > /tmp/t.log 2>&1; grep -nE "Tests finished|FAIL" /tmp/t.log`
Expected: PASS (3 tests).

- [ ] **Step 6: Commit**

```bash
git add src/control-plane/core/src/ontology.rs src/control-plane/core/tests/link_backing.rs src/control-plane/core/BUCK
git commit -m "feat(core): LinkBacking::reversed() for inverse link traversal

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 2: `Ontology::links_to` (inbound adjacency) — trait + both adapters + contract

Adding an abstract trait method requires all implementors to satisfy it in the same change to keep the build green; the testkit contract is the test. This task is therefore atomic across core/memory/postgres/testkit.

**Files:**
- Modify: `src/control-plane/core/src/ontology.rs` (trait, after the `links` method, line 136)
- Modify: `src/control-plane/memory/src/ontology.rs` (after the `links` impl, line 78)
- Modify: `src/control-plane/postgres/src/ontology.rs` (after the `links` impl, ~line 226)
- Modify: `src/control-plane/testkit/src/lib.rs` (in `ontology_contract`)
- Regenerate: `src/control-plane/postgres/.sqlx/`

- [ ] **Step 1: Write the failing contract assertions**

In `src/control-plane/testkit/src/lib.rs`, inside `ontology_contract`, immediately after the join-table round-trip assertion block (the one ending with `"join-table backing round-trips"` at ~line 667) and before the `// link to an undefined endpoint -> NotFound.` comment, insert:

```rust
    // inbound adjacency: links_to(X) returns links whose `to` is X — the inverse of
    // `links`. `customer` (Order -> Customer, upserted to Many above) is inbound to
    // Customer; `items` (Customer -> Order, join-table) is inbound to Order.
    let customer_link = LinkDef {
        cardinality: Cardinality::Many,
        ..link.clone()
    };
    assert_eq!(
        o.links_to(&tn("Customer"), PageReq::unbounded())
            .await
            .unwrap()
            .items,
        vec![customer_link],
        "links_to returns inbound links (FK)"
    );
    assert_eq!(
        o.links_to(&tn("Order"), PageReq::unbounded())
            .await
            .unwrap()
            .items,
        vec![m2m.clone()],
        "links_to returns inbound links (join-table)"
    );
```

Then, in the "unknown-type reads -> NotFound" block (around line 698, after the `o.links(&nope, ...)` assertion), add:

```rust
    assert!(matches!(
        o.links_to(&nope, PageReq::unbounded()).await,
        Err(control_plane_core::ControlPlaneError::NotFound(_))
    ));
```

- [ ] **Step 2: Add the trait method**

In `src/control-plane/core/src/ontology.rs`, immediately after the `links` trait method (line 136), add:

```rust
    /// All links whose `to` is `name` — inbound adjacency, the inverse of [`links`].
    /// `NotFound` if the type itself is absent. The `page` request is accepted but not yet
    /// enforced; results are a single full page.
    async fn links_to(&self, name: &TypeName, page: PageReq) -> Result<Page<LinkDef>>;
```

- [ ] **Step 3: Add the memory impl**

In `src/control-plane/memory/src/ontology.rs`, immediately after the `links` impl (closing brace at line 78), add:

```rust
    async fn links_to(&self, name: &TypeName, _page: PageReq) -> Result<Page<LinkDef>> {
        let ont = self.ontology.lock().unwrap();
        if !ont.types.contains_key(&name.0) {
            return Err(ControlPlaneError::NotFound(name.0.clone()));
        }
        Ok(Page::from_full(
            ont.links.iter().filter(|l| l.to == *name).cloned().collect(),
        ))
    }
```

- [ ] **Step 4: Add the postgres impl**

In `src/control-plane/postgres/src/ontology.rs`, immediately after the `links` impl (its closing `}` at ~line 226, before `async fn resolve`), add:

```rust
    async fn links_to(&self, name: &TypeName, _page: PageReq) -> Result<Page<LinkDef>> {
        let exists: bool = sqlx::query_scalar!(
            "select exists (select 1 from ontology.object_type where name = $1)",
            name.0,
        )
        .fetch_one(&self.pool)
        .await
        .map_err(backend)?
        .unwrap_or(false);
        if !exists {
            return Err(ControlPlaneError::NotFound(name.0.clone()));
        }
        let rows = sqlx::query!(
            "select name, from_type, to_type, cardinality, backing_kind, from_column, \
                    to_column, from_key, to_key, join_table_schema, join_table_name \
             from ontology.link where to_type = $1",
            name.0,
        )
        .fetch_all(&self.pool)
        .await
        .map_err(backend)?;
        Ok(Page::from_full(
            rows.into_iter()
                .map(|r| LinkDef {
                    name: r.name,
                    from: TypeName(r.from_type),
                    to: TypeName(r.to_type),
                    cardinality: cardinality_from_str(r.cardinality.as_str()),
                    backing: backing_from_row(
                        &r.backing_kind,
                        r.from_column,
                        r.to_column,
                        r.from_key,
                        r.to_key,
                        r.join_table_schema,
                        r.join_table_name,
                    ),
                })
                .collect(),
        ))
    }
```

- [ ] **Step 5: Regenerate the sqlx cache**

Run: `./tools/sqlx-prepare.sh > /tmp/sqlx.log 2>&1; grep -nE "error|query data written|Finished" /tmp/sqlx.log`
Expected: a new `.sqlx/query-*.json` for the `where to_type = $1` query. (The `query_scalar!` exists-check is identical to `links`' and may already be cached.)

- [ ] **Step 6: Run the contract for both adapters**

Run: `buck2 test //src/control-plane/memory/... //src/control-plane/postgres/... > /tmp/t.log 2>&1; grep -nE "Tests finished|FAIL|error\[" /tmp/t.log`
Expected: PASS — the memory and postgres `ontology_contract` targets both green, plus the postgres `sqlx-cache-check` green.

- [ ] **Step 7: Commit**

```bash
git add src/control-plane/core/src/ontology.rs src/control-plane/memory/src/ontology.rs src/control-plane/postgres/src/ontology.rs src/control-plane/postgres/.sqlx src/control-plane/testkit/src/lib.rs
git commit -m "feat(control-plane): Ontology::links_to inbound adjacency (pg + memory)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 3: `Direction`/`Hop` types + `ChainQuery.path: Vec<Hop>` (forward parity)

Introduce the per-hop direction representation and migrate the chain path to it. `From<&str>`/`From<String>` (→ Forward) keep every existing forward call site compiling untouched. No inverse behavior yet — resolution still uses the forward `links` lookup, reading `hop.link`.

**Files:**
- Modify: `src/services/query-api/src/handler.rs`
- Modify: `src/services/query-api/src/http.rs` (one line in `get_linked_chain` + import)
- Create: `src/services/query-api/tests/hop_types.rs`
- Modify: `src/services/query-api/BUCK`

- [ ] **Step 1: Write the failing test**

Create `src/services/query-api/tests/hop_types.rs`:

```rust
//! `Hop` / `Direction` value-type behavior: the `From` conversions default to Forward
//! (this is what keeps existing forward call sites compiling) and Direction's Default.

use query_api::handler::{Direction, Hop};

#[test]
fn from_str_is_a_forward_hop() {
    assert_eq!(
        Hop::from("orders"),
        Hop {
            link: "orders".to_string(),
            direction: Direction::Forward,
        }
    );
}

#[test]
fn from_string_is_a_forward_hop() {
    assert_eq!(Hop::from("orders".to_string()).direction, Direction::Forward);
}

#[test]
fn direction_default_is_forward() {
    assert_eq!(Direction::default(), Direction::Forward);
}
```

- [ ] **Step 2: Wire the test target**

In `src/services/query-api/BUCK`, add next to the other pure `rust_test` targets (e.g. after the `sql-compile` target, line 261):

```python
rust_test(
    name = "hop-types",
    crate = "hop_types",
    srcs = ["tests/hop_types.rs"],
    crate_root = "tests/hop_types.rs",
    edition = "2024",
    deps = [":query-api"],
)
```

- [ ] **Step 3: Run the test to verify it fails**

Run: `buck2 test //src/services/query-api:hop-types > /tmp/t.log 2>&1; grep -nE "Tests finished|FAIL|error\[|unresolved" /tmp/t.log`
Expected: FAIL — `Hop`/`Direction` not found in `query_api::handler`.

- [ ] **Step 4: Add the types**

In `src/services/query-api/src/handler.rs`, add immediately before the `/// A governed single-hop traversal` doc comment for `LinkQuery` (line 278):

```rust
/// Direction a link hop is followed. `Forward` follows the link as defined
/// (`from -> to`); `Inverse` follows it backwards (`to -> from`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Direction {
    #[default]
    Forward,
    Inverse,
}

/// One hop in a traversal path: a link name and the direction to follow it. The `From`
/// conversions yield a Forward hop, so a bare link name (`"orders".into()`) keeps every
/// existing forward call site unchanged.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Hop {
    pub link: String,
    pub direction: Direction,
}

impl From<&str> for Hop {
    fn from(link: &str) -> Self {
        Hop {
            link: link.to_string(),
            direction: Direction::Forward,
        }
    }
}

impl From<String> for Hop {
    fn from(link: String) -> Self {
        Hop {
            link,
            direction: Direction::Forward,
        }
    }
}
```

- [ ] **Step 5: Migrate `ChainQuery.path` and the resolution loop**

In `src/services/query-api/src/handler.rs`, change the `ChainQuery` struct (lines 329-333) from `pub path: Vec<String>` to:

```rust
/// A governed multi-hop traversal: from source objects matching the position-0 filters,
/// follow `path` (an ordered list of directed hops), return the deduped final-target
/// objects. Every type in the chain is governed (Read + row-filters) and caller-filterable.
pub struct ChainQuery {
    pub from_type: String,
    pub path: Vec<Hop>,
    pub filters: Vec<ChainFilter>,
}
```

Change `read_linked_objects`' delegation (line 294) from `path: vec![q.link.clone()],` to:

```rust
            path: vec![q.link.clone().into()],
```

In `read_linked_chain`, change the loop header (line 382) from `for link_name in &q.path {` to `for hop in &q.path {` and, inside, replace the two references to `link_name` — the `.find(|l| &l.name == link_name)` (line 393) and the `UnknownLink(link_name.clone())` (line 395) — so the loop body reads:

```rust
    let mut current_name = from_name.clone();
    for hop in &q.path {
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
            .find(|l| l.name == hop.link)
            .ok_or_else(|| QueryError::UnknownLink(hop.link.clone()))?;
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
            predicates: vec![],
        });
        metas.push(HopMeta {
            otype: to_type,
            denied: t_denied,
            masked: t_masked,
        });
        current_name = to_name;
    }
```

(This is the existing body with `link_name` → `hop.link`; the inverse branch arrives in Task 4.)

- [ ] **Step 6: Fix the `get_linked_chain` HTTP construction**

In `src/services/query-api/src/http.rs`, update the import (line 6-9) to add `Hop`:

```rust
use crate::handler::{
    ChainQuery, Hop, LinkQuery, ObjectQuery, QueryDeps, QueryError, Subject, read_linked_chain,
    read_linked_objects, read_object,
};
```

Then in `get_linked_chain`, change the `ChainQuery` construction's `path,` field (line 161) to convert the parsed bare names into forward hops:

```rust
        &ChainQuery {
            from_type,
            path: path.into_iter().map(Hop::from).collect(),
            filters,
        },
```

(`resolve_chain_filters(&path, ...)` above it still receives the bare `Vec<String>` before this `into_iter` consumes it — leave that call unchanged.)

- [ ] **Step 7: Run hop-types + the existing traversal suites (regression)**

Run: `buck2 test //src/services/query-api:hop-types //src/services/query-api:link-traversal //src/services/query-api:multi-hop-traversal-e2e //src/services/query-api:derived-properties-e2e > /tmp/t.log 2>&1; grep -nE "Tests finished|FAIL|error\[" /tmp/t.log`
Expected: ALL PASS — `hop-types` green; the existing fixture suites unchanged-green (their `path: vec!["orders".into(), ...]` and `LinkQuery {...}` literals compile via the new `From` impls).

- [ ] **Step 8: Commit**

```bash
git add src/services/query-api/src/handler.rs src/services/query-api/src/http.rs src/services/query-api/tests/hop_types.rs src/services/query-api/BUCK
git commit -m "refactor(query): ChainQuery.path becomes Vec<Hop> (forward parity)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 4: Inverse hop resolution in `read_linked_chain`

The feature core. Branch per-hop on direction: forward uses `links` and `link.to`; inverse uses `links_to`, requires a *unique* inbound link of that name, and follows `link.from` with `backing.reversed()`. Governance is unchanged — the reached type (`link.from` for inverse) is Read-gated and row-filtered identically.

**Files:**
- Modify: `src/services/query-api/src/handler.rs`
- Create: `src/services/query-api/tests/inverse_hops_e2e.rs`
- Modify: `src/services/query-api/BUCK`

- [ ] **Step 1: Write the failing e2e**

Create `src/services/query-api/tests/inverse_hops_e2e.rs`:

```rust
//! Inverse-direction traversal e2e: a forward FK chain Customer -> Order -> LineItem is
//! seeded, then traversed *backwards*. Inverse single-hop (Order ~orders-> Customer) and
//! inverse two-hop (LineItem ~lineItems,~orders-> Customer) both serve the correct origin
//! objects; governance still gates every reached type; an ambiguous inbound link is a
//! deterministic error and an unknown inbound link is UnknownLink.

use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use control_plane_core::{
    Action, Cardinality, DatasetRef, Effect, EventType, LineageEvent, LinkBacking, LinkDef,
    ObjectType, Ontology, PolicyTarget, PropertyDef, RoleId, RunId, SubjectId, TableRef, TypeName,
};
use control_plane_postgres::PgControlPlane;
use control_plane_postgres::fixture::{DuckLakeWriter, PgFixture};
use ingest::{MaterializeRequest, materialize};
use object_store::ObjectStore;
use object_store::local::LocalFileSystem;
use query_api::handler::{
    ChainQuery, Direction, Hop, QueryDeps, QueryError, Subject, read_linked_chain,
};
use query_api::render::objects_to_json;
use query_api::serving::EmbeddedDuckDb;
use time::OffsetDateTime;
use uuid::Uuid;

fn tref(s: &str, n: &str) -> TableRef {
    TableRef {
        schema: s.into(),
        name: n.into(),
    }
}

fn inv(link: &str) -> Hop {
    Hop {
        link: link.into(),
        direction: Direction::Inverse,
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

/// Seed customer -> orders -> line_items (the same shape as the forward multi-hop e2e),
/// define the three types and the two forward FK links `orders` and `lineItems`, attach
/// the engine. Keep the returned `DuckLakeWriter` alive (its TempDir holds the Parquet).
async fn setup(fx: &PgFixture) -> (PgControlPlane, EmbeddedDuckDb, DuckLakeWriter) {
    let (cp, db) = fx.fresh_db().await;
    let writer = DuckLakeWriter::new(fx.socket_path(), &db);
    writer.bootstrap().await;
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(writer.data_path()).unwrap());

    // customer(id, region): (1,'CA'), (2,'NY')
    let cust = tref("main", "customer");
    let cust_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("region", DataType::Utf8, true),
    ]));
    let cust_batch = RecordBatch::try_new(
        cust_schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec![Some("CA"), Some("NY")])),
        ],
    )
    .unwrap();
    land(&cp, &store, &cust, cust_schema, cust_batch).await;

    // orders(id, customer_id, status): (10,1,'shipped'),(11,1,'pending'),(20,2,'shipped')
    let ord = tref("main", "orders");
    let ord_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("customer_id", DataType::Int64, false),
        Field::new("status", DataType::Utf8, true),
    ]));
    let ord_batch = RecordBatch::try_new(
        ord_schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![10, 11, 20])),
            Arc::new(Int64Array::from(vec![1, 1, 2])),
            Arc::new(StringArray::from(vec![
                Some("shipped"),
                Some("pending"),
                Some("shipped"),
            ])),
        ],
    )
    .unwrap();
    land(&cp, &store, &ord, ord_schema, ord_batch).await;

    // line_items(id, order_id, sku): (100,10,'A'),(101,10,'B'),(102,11,'C'),(200,20,'D')
    let li = tref("main", "line_items");
    let li_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("order_id", DataType::Int64, false),
        Field::new("sku", DataType::Utf8, true),
    ]));
    let li_batch = RecordBatch::try_new(
        li_schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![100, 101, 102, 200])),
            Arc::new(Int64Array::from(vec![10, 10, 11, 20])),
            Arc::new(StringArray::from(vec![
                Some("A"),
                Some("B"),
                Some("C"),
                Some("D"),
            ])),
        ],
    )
    .unwrap();
    land(&cp, &store, &li, li_schema, li_batch).await;

    cp.define_type(ObjectType {
        name: TypeName("Customer".into()),
        properties: vec![
            PropertyDef {
                name: "id".into(),
                ty: "Long".into(),
                required: true,
            },
            PropertyDef {
                name: "region".into(),
                ty: "String".into(),
                required: false,
            },
        ],
        derived: vec![],
        table: cust.clone(),
    })
    .await
    .unwrap();
    cp.define_type(ObjectType {
        name: TypeName("Order".into()),
        properties: vec![
            PropertyDef {
                name: "id".into(),
                ty: "Long".into(),
                required: true,
            },
            PropertyDef {
                name: "customer_id".into(),
                ty: "Long".into(),
                required: true,
            },
            PropertyDef {
                name: "status".into(),
                ty: "String".into(),
                required: false,
            },
        ],
        derived: vec![],
        table: ord.clone(),
    })
    .await
    .unwrap();
    cp.define_type(ObjectType {
        name: TypeName("LineItem".into()),
        properties: vec![
            PropertyDef {
                name: "id".into(),
                ty: "Long".into(),
                required: true,
            },
            PropertyDef {
                name: "order_id".into(),
                ty: "Long".into(),
                required: true,
            },
            PropertyDef {
                name: "sku".into(),
                ty: "String".into(),
                required: false,
            },
        ],
        derived: vec![],
        table: li.clone(),
    })
    .await
    .unwrap();

    cp.define_link(LinkDef {
        name: "orders".into(),
        from: TypeName("Customer".into()),
        to: TypeName("Order".into()),
        cardinality: Cardinality::Many,
        backing: LinkBacking::ForeignKey {
            from_column: "id".into(),
            to_column: "customer_id".into(),
        },
    })
    .await
    .unwrap();
    cp.define_link(LinkDef {
        name: "lineItems".into(),
        from: TypeName("Order".into()),
        to: TypeName("LineItem".into()),
        cardinality: Cardinality::Many,
        backing: LinkBacking::ForeignKey {
            from_column: "id".into(),
            to_column: "order_id".into(),
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

/// Sorted `id` values from a served object set. Note: loom renders a `Long` property as a
/// JSON *string* (not a number), so `id` is read via `as_str()` — matching the existing
/// multi-hop traversal e2e.
fn ids(rows: &query_api::handler::ObjectRows) -> Vec<String> {
    let body = objects_to_json(rows);
    let mut out: Vec<String> = body["objects"]
        .as_array()
        .unwrap()
        .iter()
        .map(|o| o["id"].as_str().unwrap().to_string())
        .collect();
    out.sort_unstable();
    out
}

#[tokio::test(flavor = "multi_thread")]
async fn inverse_single_hop_reaches_origin_customer() {
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

    // From Order, follow `orders` (Customer -> Order) INVERSE -> the Customers that own
    // an order. Orders 10,11 belong to customer 1; order 20 to customer 2 -> {1,2}.
    let rows = read_linked_chain(
        &ChainQuery {
            from_type: "Order".into(),
            path: vec![inv("orders")],
            filters: vec![],
        },
        &Subject(a),
        &deps,
    )
    .await
    .unwrap();
    assert_eq!(ids(&rows), vec!["1".to_string(), "2".to_string()]);
}

#[tokio::test(flavor = "multi_thread")]
async fn inverse_two_hop_chain_reaches_origin_customer() {
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

    // From LineItem, INVERSE `lineItems` (Order -> LineItem) -> Order, then INVERSE
    // `orders` (Customer -> Order) -> Customer. All four line items trace back to
    // customers {1,2}.
    let rows = read_linked_chain(
        &ChainQuery {
            from_type: "LineItem".into(),
            path: vec![inv("lineItems"), inv("orders")],
            filters: vec![],
        },
        &Subject(a),
        &deps,
    )
    .await
    .unwrap();
    assert_eq!(ids(&rows), vec!["1".to_string(), "2".to_string()]);
}

#[tokio::test(flavor = "multi_thread")]
async fn inverse_hop_is_governed_on_the_reached_type() {
    let fx = PgFixture::start();
    let (cp, eng, _writer) = setup(&fx).await;
    let deps = QueryDeps {
        ontology: &cp,
        acl: &cp,
        serving: &eng,
    };
    // Grant LineItem (source) and Customer (final) but NOT Order (the intermediate type
    // the inverse `lineItems` hop reaches) -> the whole traversal is Forbidden.
    let (a, role) = subject_with_role(&cp, "carol").await;
    grant_read(&cp, &role, "LineItem").await;
    grant_read(&cp, &role, "Customer").await;

    let err = read_linked_chain(
        &ChainQuery {
            from_type: "LineItem".into(),
            path: vec![inv("lineItems"), inv("orders")],
            filters: vec![],
        },
        &Subject(a),
        &deps,
    )
    .await
    .unwrap_err();
    assert!(matches!(err, QueryError::Forbidden), "got {err:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn unknown_inbound_link_is_unknown_link() {
    let fx = PgFixture::start();
    let (cp, eng, _writer) = setup(&fx).await;
    let deps = QueryDeps {
        ontology: &cp,
        acl: &cp,
        serving: &eng,
    };
    let (a, role) = subject_with_role(&cp, "dan").await;
    grant_read(&cp, &role, "Customer").await;
    grant_read(&cp, &role, "Order").await;

    // No link named `ghost` points at Order -> UnknownLink.
    let err = read_linked_chain(
        &ChainQuery {
            from_type: "Order".into(),
            path: vec![inv("ghost")],
            filters: vec![],
        },
        &Subject(a),
        &deps,
    )
    .await
    .unwrap_err();
    assert!(matches!(err, QueryError::UnknownLink(l) if l == "ghost"), "got {err:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn ambiguous_inbound_link_is_rejected() {
    let fx = PgFixture::start();
    let (cp, eng, _writer) = setup(&fx).await;
    let deps = QueryDeps {
        ontology: &cp,
        acl: &cp,
        serving: &eng,
    };
    // Define a SECOND link also named `lineItems` but from Customer -> LineItem, so two
    // links named `lineItems` are inbound to LineItem (from Order and from Customer).
    // (Keying is (name, from), so both persist.) An inverse hop over `lineItems` from
    // LineItem can't pick one deterministically -> AmbiguousLink.
    cp.define_link(LinkDef {
        name: "lineItems".into(),
        from: TypeName("Customer".into()),
        to: TypeName("LineItem".into()),
        cardinality: Cardinality::Many,
        backing: LinkBacking::ForeignKey {
            from_column: "id".into(),
            to_column: "order_id".into(),
        },
    })
    .await
    .unwrap();

    let (a, role) = subject_with_role(&cp, "erin").await;
    grant_read(&cp, &role, "LineItem").await;
    grant_read(&cp, &role, "Order").await;
    grant_read(&cp, &role, "Customer").await;

    let err = read_linked_chain(
        &ChainQuery {
            from_type: "LineItem".into(),
            path: vec![inv("lineItems")],
            filters: vec![],
        },
        &Subject(a),
        &deps,
    )
    .await
    .unwrap_err();
    assert!(
        matches!(err, QueryError::AmbiguousLink(l) if l == "lineItems"),
        "got {err:?}"
    );
}
```

- [ ] **Step 2: Wire the test target**

In `src/services/query-api/BUCK`, add a `loom_fixture_test` mirroring `multi-hop-traversal-e2e` (lines 180-198):

```python
loom_fixture_test(
    name = "inverse-hops-e2e",
    crate = "inverse_hops_e2e",
    srcs = ["tests/inverse_hops_e2e.rs"],
    crate_root = "tests/inverse_hops_e2e.rs",
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

- [ ] **Step 3: Run the e2e to verify it fails**

Run: `buck2 test //src/services/query-api:inverse-hops-e2e > /tmp/t.log 2>&1; grep -nE "Tests finished|FAIL|error\[|AmbiguousLink|no variant" /tmp/t.log`
Expected: FAIL — `QueryError::AmbiguousLink` does not exist, and the inverse hops are resolved via the forward `links` lookup so they don't reach the origin (or 404).

- [ ] **Step 4: Add the `AmbiguousLink` error variant**

In `src/services/query-api/src/handler.rs`, in the `QueryError` enum, add after the `UnknownLink` variant (line 45-46):

```rust
    /// More than one inbound link shares the requested name (links are keyed by
    /// `(name, from)`, so `(name, to)` need not be unique). A governed read cannot pick
    /// one deterministically.
    #[error("ambiguous inbound link: {0}")]
    AmbiguousLink(String),
```

- [ ] **Step 5: Add the per-direction resolution branch**

In `src/services/query-api/src/handler.rs`, replace the body of the `for hop in &q.path` loop (the block written in Task 3, Step 5) so the link + reached-type-name + backing are chosen by direction; the governance/projection tail is unchanged:

```rust
    let mut current_name = from_name.clone();
    for hop in &q.path {
        let (next_name, backing) = match hop.direction {
            Direction::Forward => {
                let links = deps
                    .ontology
                    .links(&current_name, PageReq::unbounded())
                    .await
                    .map_err(|e| match e {
                        ControlPlaneError::NotFound(_) => {
                            QueryError::UnknownType(current_name.0.clone())
                        }
                        other => QueryError::ControlPlane(other),
                    })?;
                let link = links
                    .items
                    .into_iter()
                    .find(|l| l.name == hop.link)
                    .ok_or_else(|| QueryError::UnknownLink(hop.link.clone()))?;
                (link.to.clone(), link.backing.clone())
            }
            Direction::Inverse => {
                let links = deps
                    .ontology
                    .links_to(&current_name, PageReq::unbounded())
                    .await
                    .map_err(|e| match e {
                        ControlPlaneError::NotFound(_) => {
                            QueryError::UnknownType(current_name.0.clone())
                        }
                        other => QueryError::ControlPlane(other),
                    })?;
                let mut matches = links.items.into_iter().filter(|l| l.name == hop.link);
                let link = matches
                    .next()
                    .ok_or_else(|| QueryError::UnknownLink(hop.link.clone()))?;
                if matches.next().is_some() {
                    return Err(QueryError::AmbiguousLink(hop.link.clone()));
                }
                // Inverse: follow the link to its origin, with the backing column roles
                // swapped so the symmetric chain compiler joins `current` back to `from`.
                (link.from.clone(), link.backing.reversed())
            }
        };
        let next_target = PolicyTarget::Type(next_name.clone());
        // Read on every reached type (the leak-free guarantee), forward or inverse.
        if deps.acl.check(&subject.0, Action::Read, &next_target).await? == Decision::Deny {
            return Err(QueryError::Forbidden);
        }
        // A link pointing at a missing type is an internal inconsistency, not a 404.
        let next_type = deps.ontology.get_type(&next_name).await?;
        let (t_filters, t_denied, t_masked) =
            load_policy(deps.acl, &subject.0, &next_target).await?;
        hops.push(backing);
        ctypes.push(crate::sql::ChainType {
            table: next_type.table.clone(),
            row_filters: t_filters,
            predicates: vec![],
        });
        metas.push(HopMeta {
            otype: next_type,
            denied: t_denied,
            masked: t_masked,
        });
        current_name = next_name;
    }
```

- [ ] **Step 6: Run the e2e to verify it passes**

Run: `buck2 test //src/services/query-api:inverse-hops-e2e > /tmp/t.log 2>&1; grep -nE "Tests finished|FAIL|error\[" /tmp/t.log`
Expected: PASS (5 tests).

- [ ] **Step 7: Run the forward regression suites again**

Run: `buck2 test //src/services/query-api:link-traversal //src/services/query-api:multi-hop-traversal-e2e > /tmp/t.log 2>&1; grep -nE "Tests finished|FAIL" /tmp/t.log`
Expected: PASS — forward traversal unaffected by the direction branch.

- [ ] **Step 8: Commit**

```bash
git add src/services/query-api/src/handler.rs src/services/query-api/tests/inverse_hops_e2e.rs src/services/query-api/BUCK
git commit -m "feat(query): inverse-direction hop resolution in read_linked_chain

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 5: HTTP edge — `?direction=` and `~` path sigils

Parse direction at the HTTP boundary into the structured `Hop` representation. Pure parse helpers (unit-tested without axum), then wire them into the single-hop and chain handlers and map `AmbiguousLink` → 400.

**Files:**
- Create: `src/services/query-api/src/path_parse.rs`
- Modify: `src/services/query-api/src/lib.rs`
- Modify: `src/services/query-api/src/http.rs`
- Create: `src/services/query-api/tests/path_parse.rs`
- Modify: `src/services/query-api/BUCK`

- [ ] **Step 1: Write the failing parse-helper test**

Create `src/services/query-api/tests/path_parse.rs`:

```rust
//! Pure HTTP-edge parsing of traversal direction: `~`-prefixed path elements (multi-hop)
//! and the single-hop `?direction=` value.

use query_api::handler::{Direction, Hop};
use query_api::path_parse::{parse_direction, parse_path_hops};

#[test]
fn bare_names_are_forward_hops() {
    assert_eq!(
        parse_path_hops("orders,lineItems"),
        vec![Hop::from("orders"), Hop::from("lineItems")]
    );
}

#[test]
fn tilde_prefix_is_an_inverse_hop() {
    assert_eq!(
        parse_path_hops("~orders"),
        vec![Hop {
            link: "orders".into(),
            direction: Direction::Inverse,
        }]
    );
}

#[test]
fn mixed_directions_and_whitespace_and_empties() {
    // leading/trailing spaces trimmed (before and after the sigil); empty segments dropped.
    assert_eq!(
        parse_path_hops(" ~orders , lineItems ,, "),
        vec![
            Hop {
                link: "orders".into(),
                direction: Direction::Inverse,
            },
            Hop {
                link: "lineItems".into(),
                direction: Direction::Forward,
            },
        ]
    );
}

#[test]
fn direction_default_and_explicit_forward() {
    assert_eq!(parse_direction(None), Ok(Direction::Forward));
    assert_eq!(parse_direction(Some("forward")), Ok(Direction::Forward));
}

#[test]
fn direction_inverse() {
    assert_eq!(parse_direction(Some("inverse")), Ok(Direction::Inverse));
}

#[test]
fn direction_invalid_is_error() {
    assert!(parse_direction(Some("sideways")).is_err());
    assert!(parse_direction(Some("")).is_err());
}
```

- [ ] **Step 2: Wire the test target**

In `src/services/query-api/BUCK`, add next to the other pure targets (e.g. after `hop-types` from Task 3):

```python
rust_test(
    name = "path-parse",
    crate = "path_parse",
    srcs = ["tests/path_parse.rs"],
    crate_root = "tests/path_parse.rs",
    edition = "2024",
    deps = [":query-api"],
)
```

- [ ] **Step 3: Run the test to verify it fails**

Run: `buck2 test //src/services/query-api:path-parse > /tmp/t.log 2>&1; grep -nE "Tests finished|FAIL|error\[|unresolved" /tmp/t.log`
Expected: FAIL — `query_api::path_parse` module does not exist.

- [ ] **Step 4: Implement the parse helpers**

Create `src/services/query-api/src/path_parse.rs`:

```rust
//! Pure parsing of traversal direction at the HTTP edge — kept socket-free so it is
//! unit-testable without axum. A multi-hop `path` element prefixed with `~` is an inverse
//! hop; a bare element is forward. The single-hop `?direction=` value is `forward`
//! (default) or `inverse`; anything else is rejected.

use crate::handler::{Direction, Hop};

/// The inverse-hop sigil on a `path` element. RFC-3986 unreserved, so no URL-encoding.
const INVERSE_SIGIL: char = '~';

/// Parse a comma-separated `path` query value into directed hops. Each element is trimmed;
/// a leading `~` (after trimming) marks the hop inverse and is stripped (then re-trimmed);
/// empty elements are dropped.
pub fn parse_path_hops(param: &str) -> Vec<Hop> {
    param
        .split(',')
        .filter_map(|raw| {
            let s = raw.trim();
            let (direction, name) = match s.strip_prefix(INVERSE_SIGIL) {
                Some(rest) => (Direction::Inverse, rest.trim()),
                None => (Direction::Forward, s),
            };
            if name.is_empty() {
                None
            } else {
                Some(Hop {
                    link: name.to_string(),
                    direction,
                })
            }
        })
        .collect()
}

/// An unrecognized single-hop `direction` value.
#[derive(Debug, thiserror::Error, PartialEq)]
#[error("invalid direction '{0}' (expected 'forward' or 'inverse')")]
pub struct InvalidDirection(pub String);

/// Parse the single-hop `?direction=` value. Absent or `forward` -> Forward; `inverse`
/// -> Inverse; anything else (including empty) -> error.
pub fn parse_direction(raw: Option<&str>) -> Result<Direction, InvalidDirection> {
    match raw {
        None | Some("forward") => Ok(Direction::Forward),
        Some("inverse") => Ok(Direction::Inverse),
        Some(other) => Err(InvalidDirection(other.to_string())),
    }
}
```

- [ ] **Step 5: Export the module**

In `src/services/query-api/src/lib.rs`, add the module declaration next to the other `pub mod` lines (after `pub mod handler;` or alphabetically adjacent):

```rust
pub mod path_parse;
```

- [ ] **Step 6: Run the parse test to verify it passes**

Run: `buck2 test //src/services/query-api:path-parse > /tmp/t.log 2>&1; grep -nE "Tests finished|FAIL" /tmp/t.log`
Expected: PASS (6 tests).

- [ ] **Step 7: Wire the helpers into the HTTP handlers**

In `src/services/query-api/src/http.rs`:

(a) Update imports: add `Direction` to the handler import and `parse_direction`/`parse_path_hops`:

```rust
use crate::handler::{
    ChainQuery, Direction, Hop, LinkQuery, ObjectQuery, QueryDeps, QueryError, Subject,
    read_linked_chain, read_linked_objects, read_object,
};
use crate::path_parse::{parse_direction, parse_path_hops};
```

(b) Rewrite `get_linked` (single hop) to pull `direction` out of the params, build a one-`Hop` `ChainQuery`, and call `read_linked_chain`. Replace the whole body from the `let subject = …` line through the `read_linked_objects(…)` match with:

```rust
    let subject = headers
        .get("X-Loom-Subject")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("anonymous")
        .to_string();
    // Pull `direction` (single-hop knob) out of the params; everything else is a filter.
    let mut direction_raw: Option<String> = None;
    let mut filter_params: Vec<(String, String)> = Vec::with_capacity(params.len());
    for (k, v) in params {
        if k == "direction" {
            direction_raw = Some(v);
        } else {
            filter_params.push((k, v));
        }
    }
    let direction = match parse_direction(direction_raw.as_deref()) {
        Ok(d) => d,
        Err(e) => return (StatusCode::BAD_REQUEST, e.to_string()).into_response(),
    };
    // Resolve filter keys against the single-link path: bare -> source (t_0), `<link>.col`
    // -> target (t_1). A bad prefix -> 400. (Filter keys use the bare link name.)
    let filters = match crate::chain_filter::resolve_chain_filters(
        std::slice::from_ref(&link_name),
        filter_params,
    ) {
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
            path: vec![Hop {
                link: link_name,
                direction,
            }],
            filters,
        },
        &Subject(SubjectId(subject)),
        &deps,
    )
    .await
    {
        Ok(rows) => Json(crate::render::objects_to_json(&rows)).into_response(),
        Err(QueryError::UnknownType(t)) => (StatusCode::NOT_FOUND, t).into_response(),
        Err(QueryError::UnknownLink(l)) => (StatusCode::NOT_FOUND, l).into_response(),
        Err(QueryError::AmbiguousLink(l)) => (StatusCode::BAD_REQUEST, l).into_response(),
        Err(QueryError::BadChain(m)) => (StatusCode::BAD_REQUEST, m).into_response(),
        Err(QueryError::Forbidden) => StatusCode::FORBIDDEN.into_response(),
        Err(QueryError::BadFilter(c)) => (StatusCode::BAD_REQUEST, c).into_response(),
        Err(_) => (StatusCode::INTERNAL_SERVER_ERROR, "internal error").into_response(),
    }
```

(c) In `get_linked_chain`, replace the bare `path` parse + filter split (the `for (k, v) in params { if k == "path" { … } }` loop and the `path: Vec<String>` it builds) with sigil-aware parsing, and add the `AmbiguousLink` arm. The body becomes:

```rust
    let subject = headers
        .get("X-Loom-Subject")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("anonymous")
        .to_string();
    // `path` is the comma-separated ordered chain of (optionally `~`-inverse) link names;
    // every other pair is a filter. Repeated filter keys are preserved (e.g. a range).
    let mut hops: Vec<Hop> = Vec::new();
    let mut filter_params: Vec<(String, String)> = Vec::with_capacity(params.len());
    for (k, v) in params {
        if k == "path" {
            hops = parse_path_hops(&v);
        } else {
            filter_params.push((k, v));
        }
    }
    // Filter keys reference bare link names; resolve against those (direction-independent).
    let names: Vec<String> = hops.iter().map(|h| h.link.clone()).collect();
    let filters = match crate::chain_filter::resolve_chain_filters(&names, filter_params) {
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
            path: hops,
            filters,
        },
        &Subject(SubjectId(subject)),
        &deps,
    )
    .await
    {
        Ok(rows) => Json(crate::render::objects_to_json(&rows)).into_response(),
        Err(QueryError::UnknownType(t)) => (StatusCode::NOT_FOUND, t).into_response(),
        Err(QueryError::UnknownLink(l)) => (StatusCode::NOT_FOUND, l).into_response(),
        Err(QueryError::AmbiguousLink(l)) => (StatusCode::BAD_REQUEST, l).into_response(),
        Err(QueryError::BadChain(m)) => (StatusCode::BAD_REQUEST, m).into_response(),
        Err(QueryError::Forbidden) => StatusCode::FORBIDDEN.into_response(),
        Err(QueryError::BadFilter(c)) => (StatusCode::BAD_REQUEST, c).into_response(),
        Err(_) => (StatusCode::INTERNAL_SERVER_ERROR, "internal error").into_response(),
    }
```

Note: `get_linked` no longer uses `read_linked_objects`/`LinkQuery`, but they remain part of the handler API (used by `link-traversal` tests), so the `read_linked_objects`/`LinkQuery` imports stay. If the compiler warns they're unused **in http.rs**, drop only those two names from the `http.rs` import list (keep `read_linked_chain`); do NOT delete the handler definitions.

- [ ] **Step 8: Build query-api + run the HTTP smoke test**

Run: `buck2 test //src/services/query-api:path-parse //src/services/query-api:http-smoke > /tmp/t.log 2>&1; grep -nE "Tests finished|FAIL|error\[|warning: unused" /tmp/t.log`
Expected: PASS, and no `unused import` warnings from `http.rs` (fix per the Step 7 note if any appear).

- [ ] **Step 9: Commit**

```bash
git add src/services/query-api/src/path_parse.rs src/services/query-api/src/lib.rs src/services/query-api/src/http.rs src/services/query-api/tests/path_parse.rs src/services/query-api/BUCK
git commit -m "feat(query): parse ?direction= and ~path sigils at the HTTP edge

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 6: Docs — roadmap, FUTURE.md, sql.rs note

Record the slice as delivered and document the compiler's direction-agnosticism.

**Files:**
- Modify: `docs/superpowers/specs/2026-06-06-loom-roadmap.md`
- Modify: `docs/FUTURE.md`
- Modify: `src/services/query-api/src/sql.rs` (doc note)

- [ ] **Step 1: Add the sql.rs doc note**

In `src/services/query-api/src/sql.rs`, in the `compile_chain_with` doc comment (the block above `pub fn compile_chain_with`, starting line 411), append a sentence after the existing precondition paragraph:

```rust
/// The compiler is **direction-agnostic**: an inverse hop is expressed entirely by the
/// caller passing that hop's `LinkBacking::reversed()` in `hops` and the link's origin
/// type in `types` — the symmetric `from_alias.from_column = to_alias.to_column` join is
/// unchanged.
```

- [ ] **Step 2: Update the roadmap**

In `docs/superpowers/specs/2026-06-06-loom-roadmap.md`, find the three sentences in the "Where we are" section that read `The remaining slice-C parts are inverse-direction hops, object-set inputs, and the source→target association.` (there are two such occurrences) and the candidate-next-slices sentence. Update them to reflect inverse hops as delivered:

- Change every `The remaining slice-C parts are inverse-direction hops, object-set inputs, and the source→target association.` to `The remaining slice-C parts are object-set inputs and the source→target association.`
- In the `Candidate next slices:` sentence, change `those slice-C parts, programmatic transforms, and overwrite/incremental output.` to `the remaining slice-C parts (object-set inputs, source→target association), programmatic transforms, and overwrite/incremental output.`

Then add, immediately after the `**Comparison / set operators** …` paragraph (the end of the filter-arc paragraph), a new paragraph:

```markdown
**Inverse-direction hops** (slice-C part-3,
`2026-06-16-inverse-direction-hops-design.md`) are now delivered: every link is
traversable backwards (`target <--link-- source`), governed at every hop, across the
single-hop (`?direction=inverse`) and multi-hop (`~`-prefixed `path` elements, freely
mixed with forward hops) read paths. The control plane gained an inbound-adjacency query
(`Ontology::links_to`) and `LinkBacking::reversed()`; the SQL chain compiler is unchanged
(an inverse hop is just a reversed backing + origin type). An inbound link name that is
not unique for the target type is a deterministic 400 (`AmbiguousLink`). The remaining
slice-C parts are object-set inputs and the source→target association.
```

- [ ] **Step 3: Update FUTURE.md**

In `docs/FUTURE.md`, locate the query/traversal follow-ups section. If an "inverse-direction hops" item is listed as a pending follow-up, move it to the delivered set / mark it delivered with the spec reference `2026-06-16-inverse-direction-hops-design.md`. If no such explicit item exists, add a short delivered-note line under the query read-path follow-ups:

```markdown
- **Inverse-direction hops** (slice-C part-3) — DELIVERED
  (`docs/superpowers/specs/2026-06-16-inverse-direction-hops-design.md`): backward link
  traversal, governed at every hop, single- and multi-hop, via `Ontology::links_to` +
  `LinkBacking::reversed()`. Follow-up still open: a `/graph` surface for cyclic /
  self-link / repeated-link per-hop filtering (the deferred relational-vs-graph boundary).
```

(Read the surrounding FUTURE.md structure first and match its existing bullet style/heading; the exact wording above is a template — keep the spec path and the DELIVERED marker.)

- [ ] **Step 4: Verify the docs lint clean**

Run: `buck2 run //tools:prek -- run --files docs/superpowers/specs/2026-06-06-loom-roadmap.md docs/FUTURE.md > /tmp/lint.log 2>&1; grep -nE "Failed|Passed|error" /tmp/lint.log`
Expected: the `trim trailing whitespace` and `fix end of files` hooks Pass (no diff). If a hook rewrites a file, re-`git add` it.

- [ ] **Step 5: Commit**

```bash
git add docs/superpowers/specs/2026-06-06-loom-roadmap.md docs/FUTURE.md src/services/query-api/src/sql.rs
git commit -m "docs(query): inverse-direction hops (slice-C part-3) delivered

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Final verification (after all tasks)

- [ ] **Whole query-api + control-plane suites green:**

Run: `buck2 test //src/control-plane/... //src/services/query-api/... > /tmp/final.log 2>&1; grep -nE "Tests finished|FAIL|error\[" /tmp/final.log`
Expected: all PASS, including `link-backing`, both `ontology_contract`s, `sqlx-cache-check`, `hop-types`, `path-parse`, `inverse-hops-e2e`, and the unchanged forward suites.

- [ ] **Clippy clean across first-party Rust:**

Run: `./tools/clippy-all.sh > /tmp/clippy.log 2>&1; grep -nE "error|warning" /tmp/clippy.log`
Expected: no errors/warnings.

---

## Self-Review notes (author)

- **Spec coverage:** `LinkBacking::reversed()` (Task 1) ✓; `Ontology::links_to` + adapters + contract (Task 2) ✓; `Direction`/`Hop` + `ChainQuery.path: Vec<Hop>` + `From` conversions (Task 3) ✓; inverse resolution + `AmbiguousLink` + governance (Task 4) ✓; `?direction=`/`~` edge parsing + 400 mapping (Task 5) ✓; cardinality (handled by existing `DISTINCT`, noted, no code) ✓; docs + sql.rs note (Task 6) ✓.
- **Type consistency:** `Hop { link: String, direction: Direction }`, `Direction::{Forward, Inverse}`, `QueryError::AmbiguousLink(String)`, `parse_path_hops(&str) -> Vec<Hop>`, `parse_direction(Option<&str>) -> Result<Direction, InvalidDirection>`, `links_to(&self, &TypeName, PageReq) -> Result<Page<LinkDef>>` — used identically across tasks.
- **No placeholders:** every code step is verbatim; the only non-verbatim step is FUTURE.md (Task 6 Step 3), which is a template because that file's exact current structure must be matched in-place — the implementer is told to read it first and keep the spec path + DELIVERED marker.
- **Build atomicity:** the trait-method addition (Task 2) lands with both adapter impls + the testkit contract in one commit, so no intermediate red build. The `ChainQuery.path` type change (Task 3) lands with the `From` impls and the one `http.rs` construction fix in one commit, keeping all existing forward call sites compiling.
