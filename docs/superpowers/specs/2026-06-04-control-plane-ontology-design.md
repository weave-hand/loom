# Design: Control-Plane Ontology (Phase 3)

> **Status:** approved design for the control plane's third concern — the ontology.
> Sits under the umbrella roadmap (`2026-06-03-control-plane-roadmap-design.md`).
> Built in **one cycle** (unlike catalog's 2a/2b): it is a loom-owned schema with no
> external substrate, so it follows the queue's shape — trait → contract → fake →
> pg adapter + migration.

## Goal

The ontology is loom's **user-facing typed model**: object types (`Customer`,
`Order`), their properties, links between types (`Order.customer → Customer`), and
the mapping from each type to its backing DuckLake table. The `Ontology` trait
exposes this through the control-plane traits, satisfied by both the in-memory fake
and the Postgres adapter via one contract suite.

Unlike the catalog (DuckLake-owned, read-only — needed a seeding seam), the
`ontology` Postgres schema is **loom-owned**: loom reads *and writes* it. So the
trait carries the write ops and the contract **self-seeds through them** (the queue
pattern) — no seeding seam, no external substrate.

This also resolves the *substrate half* of the roadmap's "ontology authoring &
migration" open question: **the control plane is the authoring write API**. The
authoring *UX* (a CLI, generated definitions, or a manual tool) and physical-table
*migration* (propagating type changes to existing DuckLake tables/snapshots) layer
on top later and are **out of scope** here.

## Why one cycle

Ontology is a loom-owned CRUD surface over typed metadata — the same shape as the
queue (1a). No DuckDB, no hermetic external binary, no read-only seeding problem.
So it is a single trait → testkit contract → memory fake → pg adapter + `ontology`
migration cycle, in one plan/PR.

## The logical type model (the key design point)

Properties carry the ontology's **own logical type**, deliberately decoupled from
the physical DuckLake column types. A property's `ty` is loom's semantic type
(e.g. `EmailAddress`, `Currency`, `Customer.tier`), not the catalog's
`varchar`/`int64`. The ontology is a richer model layered over the physical tables:

- `Ontology` is the source of the **logical** model (types, properties, links).
- `Catalog::schema()` is the source of the **physical** truth (columns, DuckLake
  types) for a type's backing table.
- The two are bridged by `resolve(TypeName) -> TableRef` (reusing `core::TableRef`
  from the catalog). The property→column correspondence is **by name** for now
  (implicit); explicit column remapping/computed properties are a later concern.

`ty` is stored as an opaque string this cycle (a real loom type registry/enum is a
future cycle), but it is the *ontology's* vocabulary, not the physical one — that
distinction is the whole point of modelling properties separately from columns.

## Trait surface (`core`)

Domain types live in `core`; all ops `async`, returning `Result<_, ControlPlaneError>`.
Reuses `core::TableRef` (introduced by the catalog).

```rust
/// An ontology type name (e.g. "Customer").
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct TypeName(pub String);

/// A logical property of an object type. `ty` is the ontology's logical type
/// (loom's vocabulary), NOT the physical DuckLake column type.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PropertyDef {
    pub name: String,
    pub ty: String,        // opaque logical type for now
    pub required: bool,
}

/// An ontology object type: a named, propertied view bound to a physical table.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObjectType {
    pub name: TypeName,
    pub properties: Vec<PropertyDef>,  // ordered
    pub table: TableRef,               // backing DuckLake table (core::TableRef)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Cardinality {
    One,
    Many,
}

/// A directed link between two types (e.g. Order.customer -> Customer).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LinkDef {
    pub name: String,
    pub from: TypeName,
    pub to: TypeName,
    pub cardinality: Cardinality,
}

#[async_trait]
pub trait Ontology {
    /// Create or replace an object type (and its full property list). Upsert.
    async fn define_type(&self, ty: ObjectType) -> Result<()>;
    /// Create or replace a link. Both endpoint types must already exist, else
    /// `NotFound`. Upsert (keyed by `(name, from)`).
    async fn define_link(&self, link: LinkDef) -> Result<()>;

    /// Fetch a type by name. `NotFound` if absent.
    async fn get_type(&self, name: &TypeName) -> Result<ObjectType>;
    /// All defined types (order unspecified).
    async fn list_types(&self) -> Result<Vec<ObjectType>>;
    /// All links whose `from` is `name` (order unspecified). `NotFound` if the
    /// type itself is absent.
    async fn links(&self, name: &TypeName) -> Result<Vec<LinkDef>>;
    /// The physical DuckLake table backing `name`. `NotFound` if the type is absent.
    async fn resolve(&self, name: &TypeName) -> Result<TableRef>;
}
```

Decisions pinned here (the roadmap sketch was provisional):
- **Write ops on the trait** (`define_type`/`define_link`) — loom owns the schema;
  these are the authoring substrate and let the contract self-seed.
- **`actions()` is deferred** — Foundry-style parameterized actions are the least
  defined part of the model; omitted this cycle (YAGNI) and added when something
  consumes them.
- **Referential integrity on links** — `define_link` fails `NotFound` if either
  endpoint type is undefined, so the ontology is a real model, not a bag. The
  adapters check explicitly (not via a raw FK-violation mapping) so both backends
  return the same variant.
- **`resolve` stores, does not validate** — the type→table mapping is recorded;
  P3 does not check the table exists in the catalog (cross-concern validation is
  deferred).
- **Autocommit, not `Tx`** — ontology writes are their own transactions (an upsert
  that replaces a type's properties is internally atomic). Lineage (P5) remains the
  first real cross-concern `Tx`; ontology does not join `Tx` this cycle.
- **Single-tenant** — no tenant partitioning; the roadmap flags tenancy at P3/P4
  and it is deferred to when ACL (P4) forces it.

## Schema: `ontology` (new migration)

A new `ontology` schema migration ships in the postgres adapter
(`migrations/0002_ontology.sql`), applied like the queue's `0001`:

```
ontology.object_type
  name          text  primary key            -- TypeName
  table_schema  text  not null               -- backing TableRef.schema
  table_name    text  not null               -- backing TableRef.name

ontology.property
  type_name  text     not null  references ontology.object_type(name) on delete cascade
  ordinal    int      not null               -- preserves property order
  name       text     not null
  ty         text     not null               -- ontology logical type (NOT physical)
  required   boolean  not null
  primary key (type_name, ordinal)

ontology.link
  name        text  not null
  from_type   text  not null  references ontology.object_type(name) on delete cascade
  to_type     text  not null  references ontology.object_type(name)
  cardinality text  not null                 -- 'one' | 'many'
  primary key (name, from_type)
```

- **`define_type` (upsert):** within one internal transaction — upsert the
  `object_type` row, then replace the type's `property` rows (delete + re-insert
  with fresh ordinals). The in-memory fake replaces its entry under one lock.
- **`define_link`:** verify both endpoint types exist (explicit `get`, → `NotFound`
  otherwise), then upsert the `link` row. The FK is a backstop, not the error path.
- The in-memory fake needs no migration (maps behind a `Mutex`).

## Testing

Queue-style self-seeding contract (`testkit`, run against both adapters):
- `define_type` then `get_type` returns the type with properties in order and the
  backing `TableRef`; `define_type` again with changed properties **replaces** (no
  duplicate/stale properties).
- `list_types` returns all defined types.
- `resolve` returns the backing table; `get_type`/`resolve`/`links` for an unknown
  type → `NotFound`.
- `define_link` between two existing types, then `links(from)` returns it (with
  cardinality); `define_link` referencing an **undefined** endpoint → `NotFound`.
- a re-`define_link` with the same `(name, from)` replaces (upsert), not duplicates.

Assertions are on values/variants, never on backend-specific messages.

## Non-goals (this phase)

- **Actions** — deferred (see above).
- **Authoring UX & physical-table migration** — the control plane is the write API;
  the CLI/generated/manual authoring layer and propagating type changes to existing
  DuckLake tables/snapshots are out of scope.
- **Cross-concern validation** — `resolve` does not check the backing table exists
  in the catalog; property↔column correspondence is by-name and unenforced.
- **A real loom type system** — property `ty` stays an opaque string (the ontology's
  vocabulary), not a typed registry/enum.
- **Explicit property→column mapping / computed properties** — by-name only.
- **Tenancy** — single-tenant; partitioning deferred to ACL (P4).
- **`Tx` participation** — ontology writes are autocommit; cross-concern atomicity
  arrives with lineage (P5).
