# Design: governed link traversal (Step 3, query — relational reads, part 1)

> **Status:** approved design (2026-06-14). The first of three slices that make loom's
> ontology *relational* rather than a set of isolated typed tables. The control plane already
> *models* links (`LinkDef`, `Cardinality`, `Ontology::define_link`/`links`) but they are inert:
> a `LinkDef` carries no join keys, so it cannot be compiled to SQL, and the read path
> (`read_object`) is strictly single-type. This slice makes a link **resolvable** and adds a
> **governed traversal** read that joins across it. Slices B (derived/aggregate properties) and
> C (multi-hop / object-set traversal, cross-type filtering) build on this primitive and are
> out of scope here.

## Goal

Add a governed read that, given a **source type**, a **link**, and **equality filters on the
source**, returns the **linked objects of the target type** — fully governed on *both* ends.
Concretely: "from Customers in CA, get their Orders," where the caller sees only Orders the
target policy permits, only from Customers the source policy permits, with the inference leak
(probing for hidden source rows through a link) closed.

This is the load-bearing primitive: it makes links physically resolvable (FK **and** join-table
backed) and fixes the two-type governance composition once, so B and C inherit it.

## Why this matters now

The ontology stores types and links, and the read path serves single typed objects with ACL
compiled into SQL. But nothing connects two types at read time. Two gaps:

1. **`LinkDef` is not resolvable.** It has `{ name, from, to, cardinality }` — multiplicity but
   no join predicate. To turn `Order.customer → Customer` into SQL you need the physical
   columns (`orders.customer_id = customers.id`), or for many-to-many a mapping table. Without
   them a link is documentation, not a query.
2. **The read path is single-type.** `read_object` resolves one type → one table → one
   `compile_select`. There is no join, no second policy to compose.

Until links resolve and traverse, the "ontology" is a flat catalog of tables with labels. This
slice is the smallest thing that makes it relational.

## What this slice IS

- A `LinkBacking` on `LinkDef` that supports **foreign-key** (one-to-many / many-to-one) **and
  join-table** (many-to-many) links, stored and round-tripped by both control-plane adapters.
- A new query-api primitive `read_linked_objects` (+ a pure `compile_traversal` SQL builder)
  that joins source → (mapping table?) → target and returns target-type `ObjectRows`.
- **Both-ends governance:** Read required on source *and* target; the source row filter
  constrains which sources may be traversed from; the target row filter + column projection
  shape the result; source filter columns must be permitted.
- A plain-HTTP surface `GET /objects/{from_type}/links/{link}` returning typed-object JSON,
  reusing the existing renderer.

## What this slice is NOT

- **No target-side filtering** (filtering the returned targets by their own columns) — that is
  slice C.
- **No multi-hop / object-set traversal** (chaining links, returning the source→target
  association, or starting from a saved object set) — slice C.
- **No derived or aggregate properties** (`Customer.order_count`) — slice B.
- **No authoring-time physical-column validation** at `define_link` (see Decision 1).

## Design

### 1. Data model — make `LinkDef` resolvable (`control-plane`, ontology concern)

`LinkDef` gains a `backing` describing the physical join. `cardinality` is unchanged and means
the **to-side multiplicity** (how many targets per source); it drives result expectations, not
the join shape.

```rust
/// How a link is physically realized as a join.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LinkBacking {
    /// Direct equijoin `from_table.from_column = to_table.to_column`.
    /// Covers one-to-many and many-to-one (an FK living on one side).
    ForeignKey { from_column: String, to_column: String },
    /// Many-to-many through a mapping table:
    ///   from_table.from_key = join.from_column AND join.to_column = to_table.to_key
    JoinTable {
        table: TableRef,
        from_key: String,    // key column on the from-type's table
        from_column: String, // column on the mapping table referencing from_key
        to_column: String,   // column on the mapping table referencing to_key
        to_key: String,      // key column on the to-type's table
    },
}

pub struct LinkDef {
    pub name: String,
    pub from: TypeName,
    pub to: TypeName,
    pub cardinality: Cardinality, // to-side multiplicity (One | Many)
    pub backing: LinkBacking,     // NEW
}
```

Touches: `core/ontology.rs` (the types), the `memory` adapter (store the backing), the
`postgres` adapter + a migration (persist the backing — a discriminator + columns, or a JSON
column; follow the concern's existing column style), and the **testkit ontology contract**,
which self-seeds a link of each backing variant and asserts `links()` round-trips it. The
existing `define_link` validation (both endpoint types must exist) is unchanged.

### 2. The traversal primitive (`query-api`, new units beside the existing read)

```rust
/// A governed traversal: from source objects matching `source_filters`, follow `link`,
/// return the linked target objects.
pub struct LinkQuery {
    pub from_type: String,
    pub link: String,
    pub source_filters: Vec<(String, SqlValue)>, // eq-filters on the SOURCE type
}

pub async fn read_linked_objects(
    q: &LinkQuery,
    subject: &Subject,
    deps: &QueryDeps<'_>,
) -> Result<ObjectRows, QueryError>;
```

Flow:

1. **Resolve** the link: `get_type(from)`, `links(from)` → find the `LinkDef` by name (miss →
   `UnknownLink`) → its `to` type; resolve both `ObjectType`s (tables + ordered properties).
2. **Govern both ends:** `acl.check(subject, Read, Type(from))` and `…Type(to)` — either `Deny`
   → `Forbidden`, returned before revealing whether the type/link exists. Load each type's ACL
   policy → row filter + denied columns.
3. **Validate** every `source_filters` column is in the source's *allowed* set (source
   properties minus source-denied columns) — else `BadFilter`.
4. **Compile** `compile_traversal` → one SQL string + bound params.
5. **Execute** on the serving engine → `ObjectRows` of the **target** type (the same shape
   `read_object` returns), rendered by the existing typed-JSON renderer.

`read_object` / `compile_select` are untouched. The shared resolve + ACL boilerplate (Read
gate, row-filter load, column projection, permitted-filter-columns) is extracted into small
helpers both paths use (Approach 2).

### 3. Governance composition (the semantic core)

A traversal touches two governed types; all four rules below are enforced in the compiled SQL:

- **Read on both** `from` and `to` (deny-by-default; `Forbidden` before existence is revealed).
- **Source row filter AND-ed into the join** — you can only traverse *from* source rows you are
  permitted to see. This closes the inference leak: without it, a subject lacking Read on the
  source could still probe for hidden source rows (and their relationships) through the link.
- **Target row filter AND-ed** — only permitted targets are returned.
- **Target column projection** — `SELECT`s only the subject's allowed target columns.
- **Source filter columns must be permitted** on the source (validated in step 3 above).

### 4. SQL compilation (`compile_traversal`, pure + unit-tested)

Foreign-key backing:

```sql
SELECT DISTINCT t.<allowed cols>
FROM <to_table> t
JOIN <from_table> f ON f.<from_column> = t.<to_column>
WHERE <source eq-filters on f> AND <source row filter on f> AND <target row filter on t>
```

Join-table backing:

```sql
SELECT DISTINCT t.<allowed cols>
FROM <to_table> t
JOIN <join_table> j ON j.<to_column> = t.<to_key>
JOIN <from_table> f ON f.<from_key> = j.<from_column>
WHERE <source eq-filters on f> AND <source row filter on f> AND <target row filter on t>
```

All identifiers are quoted; **all values are bound parameters** (reusing the injection-safe
`SqlValue` binding and the `RowFilter → SQL` compilation from `compile_select`). Dedup is
`SELECT DISTINCT` over the allowed target columns (see Decision 2).

### 5. HTTP surface (`query-api`)

`GET /objects/{from_type}/links/{link}` with source eq-filters as query parameters, mirroring
the existing `GET /objects/{type}`. Subject comes from the same request header. Reuses
`AppState` and the router. Returns target-type typed-object JSON (`{ "objects": [ … ] }`).

### 6. Errors

Extend `QueryError` with `UnknownLink(String)`; reuse `UnknownType` / `Forbidden` / `BadFilter`
/ `ControlPlane` / `Serving` / `Malformed`. A link whose `from` matches but whose `name` is
absent is `UnknownLink`; a genuine backend fault propagates as itself (→ 500), never masked as
an unknown link.

## Decisions

**Decision 1 — defer authoring-time physical-column validation.** `define_link` stores the
backing without checking that the named columns exist in the physical tables. Validating them
would couple the ontology *write* path to a catalog-schema read (which `bind` does, but `bind`
is explicitly a physical-conformance gate; `define_link` is not). A bad column surfaces as an
error at traversal time. Deeper authoring-time validation is a follow-up.

**Decision 2 — dedup on the visible projection.** Many-to-many traversal can return the same
target twice. We dedup with `SELECT DISTINCT <allowed target columns>` — the *visible
projection*, not a raw key. Rationale: a subject perceives an object only through its visible
columns, and the target key column may itself be ACL-denied; deduping on the projection avoids
exposing a hidden key and is consistent with the governed view a subject is allowed to see.
True object-identity dedup needs a visible primary key, which ties into slice B and is
deferred. (Consequence: two distinct targets with identical *visible* columns collapse to one
row — acceptable, since the subject cannot distinguish them anyway.)

**Decision 3 — directed, single-link, source-filter traversal.** Part-1 traverses one named
link in its stored direction, from sources selected by an eq-filter, returning the unioned
deduped target set. Inverse traversal, target-side filtering, multi-hop, and object-set
semantics are slice C.

## Testing

- **Ontology contract** (memory + pg, one suite): `define_link` / `links` round-trip both
  `LinkBacking` variants; endpoint-existence validation unchanged.
- **`compile_traversal` unit tests** (pure): FK and join-table SQL shape; identifier quoting;
  all values bound; both row filters AND-ed; projection limited to allowed columns; `DISTINCT`.
- **Governed handler tests** (Postgres+DuckDB fixture, mirroring `governed_read`):
  - FK happy path (Customer → Orders) and join-table happy path (many-to-many).
  - `Forbidden` when the subject lacks Read on the **source**.
  - `Forbidden` when the subject lacks Read on the **target**.
  - Source row filter hides ungranted source rows — the **leak test** (a source row the subject
    may not see contributes no targets).
  - Target row filter hides ungranted targets; target projection drops denied columns.
  - `BadFilter` when a source filter names a denied column.
  - **Dedup**: two source rows linking the same target (m2m) yield that target once.
- **e2e**: extend the materialize → bind → read e2e — land two tables, `define_link` with a
  backing, traverse, assert governed, deduped, typed-object JSON.

## Follow-ups (later slices)

- **Slice B — derived / aggregate properties** (`Customer.order_count`), which likely promotes a
  real per-type primary key (and with it, key-based object identity for dedup).
- **Slice C — multi-hop / object-set traversal, inverse links, target-side filtering, the
  source→target association in the result.**
- Authoring-time physical-column validation at `define_link` (Decision 1).
- Many-to-many backed by an implicit (loom-managed) mapping table rather than a user-supplied
  one.

## Roadmap

Lands under Step 3 → Query / read path as the **part-1 of richer read capability**
(`docs/superpowers/specs/2026-06-06-loom-roadmap.md`). It also consumes the long-dormant
`Ontology::define_link` / `links` surface, giving links their first real use.
