# Control-Plane Ontology (Phase 3) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** loom's read+write `Ontology` concern — the typed object model (types, properties, links, type→table mapping) over a loom-owned `ontology` Postgres schema — exposed through the control-plane traits and satisfied by both the in-memory fake and the Postgres adapter via one self-seeding contract.

**Architecture:** Unlike the catalog (DuckLake-owned, read-only), the `ontology` schema is loom-owned, so the trait carries write ops (`define_type`/`define_link`) and the contract self-seeds through them (the queue pattern — no seeding seam, no external substrate). Properties carry the ontology's own logical type vocabulary, decoupled from physical DuckLake column types; `resolve(TypeName) -> core::TableRef` bridges to the catalog. One cycle: trait → testkit contract → memory fake → pg adapter + `ontology` migration.

**Tech Stack:** Rust 2024, buck2, `async_trait`, sqlx 0.8 (existing), the hermetic `PgFixture` + runtime `Migrator` (existing). No new third-party crates.

---

## Background for the implementer

loom's control plane is a family of crates under `src/control-plane/`: `core` (traits + domain types + `ControlPlaneError`, runtime-free), `testkit` (backend-agnostic contract suites), `memory` (in-memory fake), `postgres` (sqlx adapter + migrations + hermetic fixture). Phases 1 (queue) and 2 (catalog) are merged. This is Phase 3 (ontology), one cycle.

Read the design first: `docs/superpowers/specs/2026-06-04-control-plane-ontology-design.md`. Everything needed is restated below.

Key facts:
- **`core` error model** (`src/control-plane/core/src/error.rs`): `ControlPlaneError` with a `NotFound(String)` variant; `pub type Result<T>`. Use `NotFound` for unknown types and undefined link endpoints. Contracts assert on the **variant**, never the message.
- **`core` module/export style** (`src/control-plane/core/src/lib.rs`): `mod catalog; mod error; mod queue; mod transaction;` re-exported via `pub use`. You add a `mod ontology;`. **Reuse `core::TableRef`** (defined in `catalog.rs`) — do not redefine it.
- **`core` is runtime-free** — `ontology.rs` uses only `async_trait` (already a dep), no tokio.
- **Self-seeding contract**: because the trait has write ops, the contract is generic over just `O: Ontology` and seeds via `define_type`/`define_link` (like the queue, unlike the catalog which needed a `CatalogSeed` seam). No seam here.
- **`MemoryControlPlane`** (`src/control-plane/memory/src/lib.rs`) already impls `Queue`, `Catalog`, `ControlPlane`; you add `Ontology`. It holds concern state behind `Arc<Mutex<…>>` fields constructed in `new`; add an ontology state field the same way.
- **`PgControlPlane`** (`src/control-plane/postgres/src/lib.rs`) wraps `pool: PgPool`; has `backend(sqlx::Error) -> ControlPlaneError`, `Row as _` in scope, and an internal-transaction pattern (`self.pool.begin()`, used by `PgTx`). Migrations live in `src/control-plane/postgres/migrations/` and are applied by the runtime `Migrator` (the fixture runs them per fresh db via `LOOM_MIGRATIONS_DIR`); the `migrations` filegroup globs `migrations/**/*.sql`, so a new `0002_ontology.sql` is picked up automatically — no BUCK change for migrations.
- **Conventions:** `cargo fmt --all` before commit (rustfmt hook checks, doesn't fix); `tools/clippy-all.sh` clean; `is_none_or` not `map_or(true, …)`. Pg tests run locally: `env -u BUCK_PREFER_REMOTE buck2 test --local-only //src/control-plane/...`.
- **No new third-party crates** ⇒ no reindeer/buckify run.
- Commit messages: Conventional Commits, body ending exactly with:
  `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>`

The domain model, fake, and contract below were validated by a throwaway prototype before this plan was written.

---

## File Structure

**Task 1 — trait, domain types, contract**
- Create: `src/control-plane/core/src/ontology.rs` — domain types + `Ontology` trait.
- Modify: `src/control-plane/core/src/lib.rs` — `mod ontology;` + re-export.
- Modify: `src/control-plane/testkit/src/lib.rs` — `ontology_contract`.

**Task 2 — in-memory fake adapter + green contract**
- Modify: `src/control-plane/memory/src/lib.rs` — ontology state + `Ontology` impl.
- Create: `src/control-plane/memory/tests/ontology.rs` — run the contract.
- Modify: `src/control-plane/memory/BUCK` — `ontology` rust_test.

**Task 3 — Postgres adapter + migration + green contract**
- Create: `src/control-plane/postgres/migrations/0002_ontology.sql`.
- Modify: `src/control-plane/postgres/src/lib.rs` — `Ontology` impl.
- Create: `src/control-plane/postgres/tests/ontology.rs` — run the contract against the fixture.
- Modify: `src/control-plane/postgres/BUCK` — `ontology` rust_test (same fixture env as `queue`).

---

## Task 1: Ontology trait, domain types, contract

**Files:**
- Create: `src/control-plane/core/src/ontology.rs`
- Modify: `src/control-plane/core/src/lib.rs`
- Modify: `src/control-plane/testkit/src/lib.rs`

- [ ] **Step 1: Write the core domain types + `Ontology` trait**

Create `src/control-plane/core/src/ontology.rs`:

```rust
//! The ontology concern: loom's user-facing typed model — object types, their
//! logical properties, links between types, and the mapping from a type to its
//! backing DuckLake table. Unlike the catalog, the `ontology` schema is
//! loom-owned: this trait reads AND writes it.
//!
//! Property types are the ontology's own logical vocabulary (e.g. `EmailAddress`),
//! deliberately decoupled from the physical DuckLake column types (which come from
//! [`crate::Catalog::schema`]). [`Ontology::resolve`] bridges a type to its
//! physical table via [`crate::TableRef`].

use async_trait::async_trait;

use crate::TableRef;
use crate::error::Result;

/// An ontology type name (e.g. "Customer").
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct TypeName(pub String);

/// A logical property of an object type. `ty` is the ontology's logical type
/// (loom's vocabulary), NOT the physical DuckLake column type.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PropertyDef {
    pub name: String,
    pub ty: String,
    pub required: bool,
}

/// An ontology object type: a named, propertied view bound to a physical table.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObjectType {
    pub name: TypeName,
    /// Ordered.
    pub properties: Vec<PropertyDef>,
    pub table: TableRef,
}

/// Link multiplicity.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Cardinality {
    One,
    Many,
}

/// A directed link between two types (e.g. `Order.customer -> Customer`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LinkDef {
    pub name: String,
    pub from: TypeName,
    pub to: TypeName,
    pub cardinality: Cardinality,
}

#[async_trait]
pub trait Ontology {
    /// Create or replace an object type and its full (ordered) property list. Upsert.
    async fn define_type(&self, ty: ObjectType) -> Result<()>;
    /// Create or replace a link, keyed by `(name, from)`. Both endpoint types must
    /// already exist, else `NotFound`. Upsert.
    async fn define_link(&self, link: LinkDef) -> Result<()>;
    /// Fetch a type by name. `NotFound` if absent.
    async fn get_type(&self, name: &TypeName) -> Result<ObjectType>;
    /// All defined types (order unspecified).
    async fn list_types(&self) -> Result<Vec<ObjectType>>;
    /// All links whose `from` is `name`. `NotFound` if the type itself is absent.
    async fn links(&self, name: &TypeName) -> Result<Vec<LinkDef>>;
    /// The physical DuckLake table backing `name`. `NotFound` if the type is absent.
    async fn resolve(&self, name: &TypeName) -> Result<TableRef>;
}
```

- [ ] **Step 2: Register and export the module**

In `src/control-plane/core/src/lib.rs`, add `mod ontology;` (alphabetical: after `mod error;`/before `mod queue;` is fine — match the file) and re-export:

```rust
pub use ontology::{Cardinality, LinkDef, ObjectType, Ontology, PropertyDef, TypeName};
```
(Preserve the module doc and existing exports.)

- [ ] **Step 3: Build core**

```bash
env -u BUCK_PREFER_REMOTE buck2 build --local-only //src/control-plane/core:core
```
Expected: builds clean.

- [ ] **Step 4: Write `ontology_contract` in testkit**

In `src/control-plane/testkit/src/lib.rs`, add (add imports `Cardinality`, `LinkDef`, `ObjectType`, `Ontology`, `PropertyDef`, `TableRef`, `TypeName` from `control_plane_core`; `async-trait` is already a testkit dep but the contract is a plain `async fn`, so it needs no attribute):

```rust
/// Contract for the `Ontology` read+write surface. Self-seeds via `define_*`
/// (loom owns the ontology schema), so it needs only an `Ontology` handle.
pub async fn ontology_contract<O: Ontology>(o: &O) {
    let tn = |s: &str| TypeName(s.to_string());
    let tref = |s: &str, n: &str| TableRef {
        schema: s.to_string(),
        name: n.to_string(),
    };

    // define + get round-trips with ordered properties and the backing table.
    let customer = ObjectType {
        name: tn("Customer"),
        table: tref("main", "customer"),
        properties: vec![PropertyDef {
            name: "email".into(),
            ty: "EmailAddress".into(),
            required: true,
        }],
    };
    o.define_type(customer.clone()).await.expect("define Customer");
    let order = ObjectType {
        name: tn("Order"),
        table: tref("main", "orders"),
        properties: vec![
            PropertyDef { name: "total".into(), ty: "Currency".into(), required: true },
            PropertyDef { name: "note".into(), ty: "Text".into(), required: false },
        ],
    };
    o.define_type(order.clone()).await.expect("define Order");

    assert_eq!(o.get_type(&tn("Order")).await.unwrap(), order, "round-trips");
    assert_eq!(
        o.get_type(&tn("Order")).await.unwrap()
            .properties.iter().map(|p| p.name.clone()).collect::<Vec<_>>(),
        vec!["total".to_string(), "note".to_string()],
        "property order preserved"
    );

    // resolve -> backing table.
    assert_eq!(o.resolve(&tn("Customer")).await.unwrap(), tref("main", "customer"));

    // list_types contains both.
    let names: std::collections::HashSet<String> = o
        .list_types().await.unwrap().into_iter().map(|t| t.name.0).collect();
    assert_eq!(
        names,
        ["Customer", "Order"].iter().map(|s| s.to_string()).collect()
    );

    // re-define replaces the property list (no stale properties).
    let order_v2 = ObjectType {
        name: tn("Order"),
        table: tref("main", "orders"),
        properties: vec![PropertyDef { name: "total".into(), ty: "Currency".into(), required: true }],
    };
    o.define_type(order_v2).await.unwrap();
    assert_eq!(
        o.get_type(&tn("Order")).await.unwrap().properties.len(),
        1,
        "redefine replaces properties"
    );

    // link between existing types, then read it back.
    let link = LinkDef {
        name: "customer".into(),
        from: tn("Order"),
        to: tn("Customer"),
        cardinality: Cardinality::One,
    };
    o.define_link(link.clone()).await.expect("define link");
    assert_eq!(o.links(&tn("Order")).await.unwrap(), vec![link.clone()]);

    // re-define same (name, from) upserts (no duplicate; cardinality updated).
    o.define_link(LinkDef { cardinality: Cardinality::Many, ..link.clone() })
        .await
        .unwrap();
    let ls = o.links(&tn("Order")).await.unwrap();
    assert_eq!(ls.len(), 1, "link upsert, not duplicate");
    assert_eq!(ls[0].cardinality, Cardinality::Many, "cardinality updated");

    // link to an undefined endpoint -> NotFound.
    assert!(
        matches!(
            o.define_link(LinkDef {
                name: "ghost".into(),
                from: tn("Order"),
                to: tn("Ghost"),
                cardinality: Cardinality::One,
            })
            .await,
            Err(control_plane_core::ControlPlaneError::NotFound(_))
        ),
        "link to undefined type is NotFound"
    );

    // unknown-type reads -> NotFound.
    let nope = tn("Nope");
    assert!(matches!(o.get_type(&nope).await, Err(control_plane_core::ControlPlaneError::NotFound(_))));
    assert!(matches!(o.resolve(&nope).await, Err(control_plane_core::ControlPlaneError::NotFound(_))));
    assert!(matches!(o.links(&nope).await, Err(control_plane_core::ControlPlaneError::NotFound(_))));
}
```

- [ ] **Step 5: Build testkit, format, lint, commit**

```bash
env -u BUCK_PREFER_REMOTE buck2 build --local-only //src/control-plane/testkit:testkit
cargo fmt --all
tools/clippy-all.sh
git add -A
git commit -m "feat(control-plane): add Ontology trait + ontology_contract (no adapter yet)"
```
(Append the Co-Authored-By trailer.)

---

## Task 2: In-memory fake adapter + green contract

**Files:**
- Modify: `src/control-plane/memory/src/lib.rs`
- Create: `src/control-plane/memory/tests/ontology.rs`
- Modify: `src/control-plane/memory/BUCK`

- [ ] **Step 1: Write the failing contract test**

Create `src/control-plane/memory/tests/ontology.rs`:

```rust
#[tokio::test]
async fn memory_passes_ontology_contract() {
    let cp = control_plane_memory::MemoryControlPlane::new(std::time::Duration::from_millis(300));
    control_plane_testkit::ontology_contract(&cp).await;
}
```

- [ ] **Step 2: Run it to confirm it fails**

```bash
env -u BUCK_PREFER_REMOTE buck2 test --local-only //src/control-plane/memory:ontology 2>&1 | tail -20
```
Expected: build failure — no `Ontology` impl and no `ontology` target yet (added in Step 4).

- [ ] **Step 3: Implement `Ontology` for the fake**

In `src/control-plane/memory/src/lib.rs`:

Add the catalog/ontology imports needed to the existing `control_plane_core` import: `Cardinality, LinkDef, ObjectType, Ontology, TypeName` (merge into the existing `use control_plane_core::{...}` line; `ControlPlaneError`, `Result` are already imported). Ensure `HashMap` is imported (it is, from the catalog work).

Add the ontology state type (module-level):
```rust
#[derive(Default)]
struct OntologyState {
    types: HashMap<String, ObjectType>,
    links: Vec<LinkDef>,
}
```

Add an `ontology` field to `MemoryControlPlane` and construct it in `new` (alongside `rows`/`notify`/`catalog`):
```rust
    ontology: Arc<Mutex<OntologyState>>,
```
```rust
            ontology: Arc::new(Mutex::new(OntologyState::default())),
```

Add the impl:
```rust
#[async_trait]
impl Ontology for MemoryControlPlane {
    async fn define_type(&self, ty: ObjectType) -> Result<()> {
        self.ontology
            .lock()
            .unwrap()
            .types
            .insert(ty.name.0.clone(), ty);
        Ok(())
    }

    async fn define_link(&self, link: LinkDef) -> Result<()> {
        let mut ont = self.ontology.lock().unwrap();
        for endpoint in [&link.from, &link.to] {
            if !ont.types.contains_key(&endpoint.0) {
                return Err(ControlPlaneError::NotFound(format!("type {}", endpoint.0)));
            }
        }
        ont.links
            .retain(|l| !(l.name == link.name && l.from == link.from));
        ont.links.push(link);
        Ok(())
    }

    async fn get_type(&self, name: &TypeName) -> Result<ObjectType> {
        self.ontology
            .lock()
            .unwrap()
            .types
            .get(&name.0)
            .cloned()
            .ok_or_else(|| ControlPlaneError::NotFound(name.0.clone()))
    }

    async fn list_types(&self) -> Result<Vec<ObjectType>> {
        Ok(self.ontology.lock().unwrap().types.values().cloned().collect())
    }

    async fn links(&self, name: &TypeName) -> Result<Vec<LinkDef>> {
        let ont = self.ontology.lock().unwrap();
        if !ont.types.contains_key(&name.0) {
            return Err(ControlPlaneError::NotFound(name.0.clone()));
        }
        Ok(ont.links.iter().filter(|l| l.from == *name).cloned().collect())
    }

    async fn resolve(&self, name: &TypeName) -> Result<TableRef> {
        Ok(self.get_type(name).await?.table)
    }
}
```
(`TableRef` is already imported in `memory/src/lib.rs` from the catalog impl; if not, add it. The unused `Cardinality` import warning, if any, means it isn't referenced here — drop it from the import. Let clippy/build guide the exact import set.)

- [ ] **Step 4: Add the memory `ontology` test target**

In `src/control-plane/memory/BUCK`, add (mirroring the `catalog` target):
```python
rust_test(
    name = "ontology",
    crate = "ontology",
    srcs = ["tests/ontology.rs"],
    crate_root = "tests/ontology.rs",
    edition = "2024",
    deps = [
        ":memory",
        "//src/control-plane/testkit:testkit",
        "//third-party:tokio",
    ],
)
```

- [ ] **Step 5: Run the contract — red→green**

```bash
env -u BUCK_PREFER_REMOTE buck2 test --local-only //src/control-plane/memory:ontology
```
Expected: `memory_passes_ontology_contract` passes.

- [ ] **Step 6: Build, format, lint, commit**

```bash
cargo fmt --all
env -u BUCK_PREFER_REMOTE buck2 test --local-only //src/control-plane/memory/...
tools/clippy-all.sh
git add -A
git commit -m "feat(control-plane): in-memory Ontology fake passing the ontology contract"
```
(Append the Co-Authored-By trailer.)

---

## Task 3: Postgres adapter + migration + green contract

**Files:**
- Create: `src/control-plane/postgres/migrations/0002_ontology.sql`
- Modify: `src/control-plane/postgres/src/lib.rs`
- Create: `src/control-plane/postgres/tests/ontology.rs`
- Modify: `src/control-plane/postgres/BUCK`

- [ ] **Step 1: Write the migration**

Create `src/control-plane/postgres/migrations/0002_ontology.sql`:

```sql
create schema if not exists ontology;

create table ontology.object_type (
    name         text primary key,
    table_schema text not null,
    table_name   text not null
);

create table ontology.property (
    type_name text    not null references ontology.object_type (name) on delete cascade,
    ordinal   int     not null,
    name      text    not null,
    ty        text    not null,
    required  boolean not null,
    primary key (type_name, ordinal)
);

create table ontology.link (
    name        text not null,
    from_type   text not null references ontology.object_type (name) on delete cascade,
    to_type     text not null references ontology.object_type (name),
    cardinality text not null,
    primary key (name, from_type)
);
```

- [ ] **Step 2: Write the failing contract test**

Create `src/control-plane/postgres/tests/ontology.rs`:

```rust
use control_plane_postgres::fixture::PgFixture;

#[tokio::test]
async fn postgres_passes_ontology_contract() {
    let fixture = PgFixture::start();
    let cp = fixture.fresh_control_plane().await;
    control_plane_testkit::ontology_contract(&cp).await;
}
```

- [ ] **Step 3: Run it to confirm it fails (no impl/target yet)**

```bash
env -u BUCK_PREFER_REMOTE buck2 test --local-only //src/control-plane/postgres:ontology 2>&1 | tail -20
```
Expected: build failure.

- [ ] **Step 4: Implement `Ontology` for `PgControlPlane`**

In `src/control-plane/postgres/src/lib.rs`, add the catalog/ontology types to the existing `control_plane_core` import (`Cardinality, LinkDef, ObjectType, Ontology, PropertyDef, TypeName` — `TableRef`, `ControlPlaneError`, `Result` already imported), then add:

```rust
fn cardinality_to_str(c: Cardinality) -> &'static str {
    match c {
        Cardinality::One => "one",
        Cardinality::Many => "many",
    }
}

fn cardinality_from_str(s: &str) -> Cardinality {
    match s {
        "many" => Cardinality::Many,
        _ => Cardinality::One,
    }
}

#[async_trait]
impl Ontology for PgControlPlane {
    async fn define_type(&self, ty: ObjectType) -> Result<()> {
        let mut tx = self.pool.begin().await.map_err(backend)?;
        sqlx::query(
            "insert into ontology.object_type (name, table_schema, table_name) \
             values ($1, $2, $3) \
             on conflict (name) do update set table_schema = excluded.table_schema, \
                 table_name = excluded.table_name",
        )
        .bind(&ty.name.0)
        .bind(&ty.table.schema)
        .bind(&ty.table.name)
        .execute(&mut *tx)
        .await
        .map_err(backend)?;
        sqlx::query("delete from ontology.property where type_name = $1")
            .bind(&ty.name.0)
            .execute(&mut *tx)
            .await
            .map_err(backend)?;
        for (i, p) in ty.properties.iter().enumerate() {
            sqlx::query(
                "insert into ontology.property (type_name, ordinal, name, ty, required) \
                 values ($1, $2, $3, $4, $5)",
            )
            .bind(&ty.name.0)
            .bind(i as i32)
            .bind(&p.name)
            .bind(&p.ty)
            .bind(p.required)
            .execute(&mut *tx)
            .await
            .map_err(backend)?;
        }
        tx.commit().await.map_err(backend)?;
        Ok(())
    }

    async fn define_link(&self, link: LinkDef) -> Result<()> {
        for endpoint in [&link.from, &link.to] {
            let exists: bool = sqlx::query_scalar(
                "select exists (select 1 from ontology.object_type where name = $1)",
            )
            .bind(&endpoint.0)
            .fetch_one(&self.pool)
            .await
            .map_err(backend)?;
            if !exists {
                return Err(ControlPlaneError::NotFound(format!("type {}", endpoint.0)));
            }
        }
        sqlx::query(
            "insert into ontology.link (name, from_type, to_type, cardinality) \
             values ($1, $2, $3, $4) \
             on conflict (name, from_type) do update set to_type = excluded.to_type, \
                 cardinality = excluded.cardinality",
        )
        .bind(&link.name)
        .bind(&link.from.0)
        .bind(&link.to.0)
        .bind(cardinality_to_str(link.cardinality))
        .execute(&self.pool)
        .await
        .map_err(backend)?;
        Ok(())
    }

    async fn get_type(&self, name: &TypeName) -> Result<ObjectType> {
        let row = sqlx::query(
            "select table_schema, table_name from ontology.object_type where name = $1",
        )
        .bind(&name.0)
        .fetch_optional(&self.pool)
        .await
        .map_err(backend)?
        .ok_or_else(|| ControlPlaneError::NotFound(name.0.clone()))?;
        let props = sqlx::query(
            "select name, ty, required from ontology.property \
             where type_name = $1 order by ordinal",
        )
        .bind(&name.0)
        .fetch_all(&self.pool)
        .await
        .map_err(backend)?;
        Ok(ObjectType {
            name: name.clone(),
            table: TableRef {
                schema: row.get("table_schema"),
                name: row.get("table_name"),
            },
            properties: props
                .iter()
                .map(|r| PropertyDef {
                    name: r.get("name"),
                    ty: r.get("ty"),
                    required: r.get("required"),
                })
                .collect(),
        })
    }

    async fn list_types(&self) -> Result<Vec<ObjectType>> {
        let names: Vec<String> = sqlx::query_scalar("select name from ontology.object_type")
            .fetch_all(&self.pool)
            .await
            .map_err(backend)?;
        let mut out = Vec::with_capacity(names.len());
        for n in names {
            out.push(self.get_type(&TypeName(n)).await?);
        }
        Ok(out)
    }

    async fn links(&self, name: &TypeName) -> Result<Vec<LinkDef>> {
        let exists: bool =
            sqlx::query_scalar("select exists (select 1 from ontology.object_type where name = $1)")
                .bind(&name.0)
                .fetch_one(&self.pool)
                .await
                .map_err(backend)?;
        if !exists {
            return Err(ControlPlaneError::NotFound(name.0.clone()));
        }
        let rows = sqlx::query(
            "select name, from_type, to_type, cardinality from ontology.link where from_type = $1",
        )
        .bind(&name.0)
        .fetch_all(&self.pool)
        .await
        .map_err(backend)?;
        Ok(rows
            .iter()
            .map(|r| LinkDef {
                name: r.get("name"),
                from: TypeName(r.get("from_type")),
                to: TypeName(r.get("to_type")),
                cardinality: cardinality_from_str(r.get::<String, _>("cardinality").as_str()),
            })
            .collect())
    }

    async fn resolve(&self, name: &TypeName) -> Result<TableRef> {
        let row = sqlx::query(
            "select table_schema, table_name from ontology.object_type where name = $1",
        )
        .bind(&name.0)
        .fetch_optional(&self.pool)
        .await
        .map_err(backend)?
        .ok_or_else(|| ControlPlaneError::NotFound(name.0.clone()))?;
        Ok(TableRef {
            schema: row.get("table_schema"),
            name: row.get("table_name"),
        })
    }
}
```

- [ ] **Step 5: Add the postgres `ontology` test target**

In `src/control-plane/postgres/BUCK`, add (same fixture env as the `queue` target — `POSTGRES_BIN_DIR`/`POSTGRES_LD_LIBRARY_PATH`/`LOOM_MIGRATIONS_DIR`; no DuckDB needed here):
```python
rust_test(
    name = "ontology",
    crate = "ontology",
    srcs = ["tests/ontology.rs"],
    crate_root = "tests/ontology.rs",
    edition = "2024",
    env = {
        "POSTGRES_BIN_DIR": "$(location :postgres-bin)/bin",
        "POSTGRES_LD_LIBRARY_PATH": "$(location :postgres-bin)/lib:$(location :libxml2)",
        "LOOM_MIGRATIONS_DIR": "$(location :migrations)/migrations",
    },
    deps = [":postgres", "//src/control-plane/testkit:testkit", "//third-party:tokio"],
)
```

- [ ] **Step 6: Run the contract — red→green**

```bash
env -u BUCK_PREFER_REMOTE buck2 test --local-only //src/control-plane/postgres:ontology 2>&1 | tail -25
```
Expected: `postgres_passes_ontology_contract` passes — the migration applies and the adapter satisfies the same contract as the fake. If a `required`/`bool` or `int` type-map errors, recheck the `r.get` targets (`required`→`bool`, `ordinal` bound as `i32`).

- [ ] **Step 7: Full build, test, format, lint**

```bash
cargo fmt --all
env -u BUCK_PREFER_REMOTE buck2 test --local-only //src/control-plane/...
tools/clippy-all.sh
```
Expected: the whole control plane builds & passes (queue, worker, catalog, ontology on both adapters); clippy clean.

- [ ] **Step 8: Commit**

```bash
git add -A
git commit -m "feat(control-plane): Postgres Ontology adapter + ontology schema migration"
```
(Append the Co-Authored-By trailer.)

---

## Final verification (after all tasks)

```bash
cargo fmt --all --check
env -u BUCK_PREFER_REMOTE buck2 test --local-only //src/...
tools/clippy-all.sh
buck2 run //tools:prek -- run --all-files
```

All green ⇒ ready to push and open a PR.

## Notes / gotchas

- **Self-seeding contract** — no seeding seam (the queue pattern), because the trait writes. `ontology_contract` takes just `&O`.
- **Reuse `core::TableRef`** — do NOT define a second `TableRef`; `resolve` returns the catalog's type (the cross-concern bridge).
- **`define_type` is an internal transaction** in pg (upsert the type row, then delete+reinsert its properties) so a re-define is atomic and leaves no stale properties.
- **Referential integrity is enforced explicitly** in both adapters (check endpoints exist → `NotFound`), not via raw FK-violation mapping, so both backends return the same variant. The pg FKs are a backstop.
- **`ty` is the ontology's logical type**, not the physical DuckLake column type — never reconcile it with `Catalog::schema()` here.
- **No reindeer/buckify** — no new third-party crates. The `0002_ontology.sql` migration is picked up by the existing `migrations` filegroup glob (no BUCK change for it).
```
