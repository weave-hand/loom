# Design: Derived properties — part 1 (aggregate-over-link)

> **Status:** approved design (2026-06-15). The next richer-reads slice (FUTURE.md "slice B") after
> governed link traversal. An ontology object type gains **derived properties** — computed,
> read-time **aggregates over an existing link** (e.g. `Customer.orderCount = COUNT` of linked
> Orders, `Customer.totalSpend = SUM` over linked Orders' `amount`) — served through the governed
> read path alongside physical properties, with both-ends ACL governance.

## Goal

Let an `ObjectType` declare **derived properties** that aggregate over one of its links, and serve
them through `read_object` next to physical properties. Reading `Customer` returns
`{ id, name, orderCount, totalSpend }` where `orderCount`/`totalSpend` are computed at read time as
correlated subqueries over the `Customer→Order` link — fully governed: the subject must be permitted
to read the linked type, and the linked type's row-filters apply inside the aggregate.

This is the second richer-reads slice (after link traversal, part-1). It reuses the
resolvable-link + governed-join primitive that slice delivered: a derived aggregate is the link's
join shape expressed as a correlated subquery in the SELECT.

## North star (context, not part-1)

Richer reads ultimately span derived/aggregate properties, scalar/computed expressions, and
multi-hop / object-set traversal. This slice delivers **aggregate-over-a-single-link** derived
properties on the primary object read. Scalar expressions, multi-hop aggregates, derived props on
traversal output, and materialization are explicit follow-ons.

## Decisions (settled in brainstorming)

- **A — Aggregate-over-link only.** A derived property aggregates over an existing ontology `LinkDef`
  (FK or join-table). No scalar/own-column expressions (a separate, SQL-surface-bearing slice).
- **B — COUNT + SUM/AVG/MIN/MAX.** `COUNT` counts linked rows (no column); `SUM/AVG/MIN/MAX` take a
  named column on the linked type. Empty-set: `COUNT`/`SUM` → 0 (COALESCE), `AVG/MIN/MAX` → null.
- **C — Both-ends governed.** To include a derived aggregate, the subject must hold `Read` on the
  **linked** type; the linked type's row-filters apply **inside** the subquery; if the subject can't
  read the linked type (or the aggregated column is denied to them), the derived property is
  **omitted** (treated like a denied column), not an error.
- **D — Separate `ObjectType.derived` collection.** `derived: Vec<DerivedPropertyDef>` beside
  `properties`; a new `ontology.derived_property` table. `PropertyDef` is untouched.

## What this slice IS

- A new ontology **`DerivedPropertyDef`** + `Aggregation` (core), stored in a new
  `ontology.derived_property` table (postgres + memory + testkit round-trip).
- A **`compile_select` extension**: derived aggregates compiled as correlated subqueries in the
  SELECT, reusing the FK/join-table join shapes.
- **`read_object` integration**: project physical then derived properties, with per-derived
  both-ends ACL resolution; output rendered through the existing typed-JSON path.

## What this slice is NOT

- **No scalar/expression derived properties** (own-column computations) — a later slice; it bears a
  SQL-expression surface to keep injection-safe and governable.
- **No derived properties on link-traversal output** — `read_object` only for part-1;
  `read_linked_objects` is unchanged.
- **No multi-hop aggregates** (aggregate over a chain of links), no nested derived-on-derived.
- **No materialization** — read-time correlated subquery only.
- **No define-time link/type validation** — the named link is resolved at read time (a missing link
  surfaces then). Authoring-time validation is a follow-on.
- **No derived props as filter/sort targets** — they are projected, not filterable.

## Design

### 1. Ontology model (core — `ontology.rs`)

```rust
/// How a derived property aggregates over its link's target rows. The String is the
/// target-type column to aggregate (COUNT takes none).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Aggregation {
    Count,
    Sum(String),
    Avg(String),
    Min(String),
    Max(String),
}

/// A computed property: aggregate `agg` over the rows reachable from this type via `link`.
/// `ty` is the declared logical type of the result (e.g. "Long" for a count, "Double" for an avg).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DerivedPropertyDef {
    pub name: String,
    pub ty: String,
    pub link: String,   // a LinkDef name whose `from` is this type
    pub agg: Aggregation,
}
```

`ObjectType` gains `pub derived: Vec<DerivedPropertyDef>` (ordered) beside `properties` and `table`.
Exported from `lib.rs`. The author declares `ty` (part-1 trusts it; the SQL produces a value and
the existing render path uses the declared type — no define-time type inference/validation).

### 2. Storage

New postgres table:

```sql
create table ontology.derived_property (
    type_name  text    not null references ontology.object_type (name) on delete cascade,
    ordinal    int     not null,
    name       text    not null,
    ty         text    not null,
    link_name  text    not null,
    agg_kind   text    not null,        -- 'count' | 'sum' | 'avg' | 'min' | 'max'
    agg_column text,                     -- null for count
    primary key (type_name, ordinal)
);
```

`define_type` delete-then-reinserts derived rows (mirroring `ontology.property`); `get_type` selects
them `order by ordinal` and reconstructs `Aggregation` from `(agg_kind, agg_column)`. Memory fake
stores `derived` on the cloned `ObjectType`. New migration + `.sqlx` regen. Testkit
`ontology_contract` gains a derived round-trip (order preserved, upsert-replaces, each agg kind).

### 3. SQL compiler — `compile_select` extension

`compile_select` gains a parameter `derived: &[DerivedAggregate<'_>]` where:

```rust
pub struct DerivedAggregate<'a> {
    pub name: &'a str,                 // output alias
    pub agg: &'a Aggregation,
    pub backing: &'a LinkBacking,      // the resolved link's join shape
    pub target_table: &'a TableRef,    // the linked type's table
    pub target_filters: &'a [RowFilter], // the linked type's ACL row-filters (both-ends)
}
```

The outer table is given an alias (`o`); physical columns stay unqualified (single-table refs
resolve under the alias). Each derived aggregate is appended to the SELECT list, after the physical
columns, as a correlated subquery:

- **`Aggregation` → SQL function:** `Count → COUNT(*)`; `Sum(c) → COALESCE(SUM(sub."c"),0)`;
  `Avg(c) → AVG(sub."c")`; `Min(c)/Max(c) → MIN/MAX(sub."c")`.
- **FK backing** `{from_column, to_column}` (source.from_column = target.to_column):
  ```sql
  (SELECT <aggfn> FROM <target_table> sub
     WHERE sub."<to_column>" = o."<from_column>" [AND <target_filters on sub>]) AS "<name>"
  ```
- **Join-table backing** `{table, from_key, from_column, to_column, to_key}`:
  ```sql
  (SELECT <aggfn> FROM <target_table> sub
     JOIN <table> j ON j."<to_column>" = sub."<to_key>"
     WHERE j."<from_column>" = o."<from_key>" [AND <target_filters on sub>]) AS "<name>"
  ```

**Parameter order:** subquery params (the target row-filters, in SELECT order) come **first** in the
returned params vector — the SELECT clause precedes the WHERE — then the outer `row_filters` +
`eq_filters` params. The column list is `physical… , derived…`; physical columns contribute no
params, so derived params lead. `quote_ident`, `filter_sql`, and the join shapes are reused from the
existing compiler; a `derived_aggregate_sql` helper builds each expression. (`compile_traversal` is
unchanged.)

### 4. `read_object` integration + both-ends governance

After projecting allowed physical columns (existing `project_allowed` over `properties`), build the
derived list:

For each `d` in `object_type.derived` (in order):
1. **Source deny/mask:** if `d.name` ∈ source `denied` → skip (omit). If ∈ source `masked` → emit it
   as a masked marker (`'***' AS "<name>"`) and do NOT compute the subquery (cheap; no target access).
2. **Resolve the link:** find the `LinkDef` named `d.link` among the source type's links
   (`ontology.links(source)`). Missing link → omit (part-1 has no define-time validation; a future
   slice validates at author time). Resolve the link's `to` type → its `ObjectType` (table).
3. **Both-ends Read gate:** `acl.check(subject, Action::Read, Type(to_type))`. `Deny` → **omit** the
   derived property (treated like a denied column — no leak).
4. **Target policy:** load the `to_type` policy. Apply its `row_filters` inside the subquery
   (`target_filters`). If `d.agg` names a column (`Sum/Avg/Min/Max`) that is in the target's
   `denied` set → **omit** (don't leak a denied column via an aggregate).
5. Otherwise build a `DerivedAggregate` for the compiler.

Pass the physical `allowed` + the masked-derived markers + the computed `DerivedAggregate`s to
`compile_select`. The output **columns** = allowed physical ++ surviving derived (in declaration
order); **logical_types** map physical→`PropertyDef.ty`, derived→`DerivedPropertyDef.ty`; rows render
through the existing typed-JSON path unchanged (`orderCount: "3"` as a Long-string, `totalSpend: 7.5`
as a Double). `read_linked_objects` is untouched.

### 5. Error handling

- A genuinely unknown source type → `UnknownType` (existing). A derived property whose link/target
  is missing or ungoverned is **silently omitted**, never an error (consistent with denied-column
  semantics — the read still succeeds with the permitted projection).
- A malformed `Aggregation` (e.g. `Sum` with an empty column) is rejected at compile via
  `CompileError` → opaque 500 (it indicates a bad `DerivedPropertyDef`, an internal/authoring fault).

### File structure

- **Core:** `DerivedPropertyDef`/`Aggregation` + `ObjectType.derived` in `ontology.rs`; `lib.rs`
  exports.
- **Postgres:** migration `00NN_derived_property.sql`; `define_type`/`get_type` derived read/write
  (+ `.sqlx`); memory fake.
- **Testkit:** derived round-trip in `ontology_contract`.
- **query-api:** `compile_select` + `DerivedAggregate` + `derived_aggregate_sql` in `sql.rs`;
  `read_object` integration in `handler.rs`; unit tests (`sql_compile.rs`) + a governed e2e
  (`derived_properties_e2e.rs` or fold into `governed_read.rs`).
- **Docs:** roadmap delivered marker; FUTURE.md follow-ups.

## Decisions

- **A** aggregate-over-link only; **B** COUNT + SUM/AVG/MIN/MAX; **C** both-ends governed
  (omit-if-unreadable); **D** separate `ObjectType.derived` collection.

## Follow-ups (later slices)

- **Scalar/expression derived properties** (own-column computations) — the whitelisted-expression slice.
- **Derived properties on traversal output** (`read_linked_objects`).
- **Multi-hop aggregates** (aggregate over a link chain) and derived-on-derived.
- **Materialization** of derived properties (vs read-time subquery) for hot paths.
- **Define-time validation** of a derived property's link/target/column/type at `define_type`.
- **Derived props as filter/sort targets.**

## Roadmap

Lands under Step 3 → Query, the richer-reads track, **slice B (derived/aggregate properties)** —
building directly on the governed link-traversal primitive: a derived aggregate is that join,
expressed as a governed correlated subquery in the object read.
