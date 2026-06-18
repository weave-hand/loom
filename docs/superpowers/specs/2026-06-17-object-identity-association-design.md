# Object identity + source→target association — design (2026-06-17)

> Query-pillar graph slice (slice-C continuation). Introduce a first-class **object
> identity** (primary key) to the ontology, then use it to deliver **source→target
> association**: a governed traversal that returns the actual edges as compact
> `{from, to}` identity pairs, rather than the `DISTINCT`-collapsed final-target set
> that `read_linked_chain` returns today.

## Motivation

A traversal today (`GET /objects/{from}/links{...}`) answers "what target objects are
reachable from the (filtered) source set?" — it `SELECT DISTINCT`s the final target's
columns and discards *which source reached which target*. That pairing — the edge list
— is the actual graph. Returning it lets a caller build an association view
("Customer 5 → Order 12, 13; Customer 6 → Order 14") instead of a flat target list.

To return edges compactly we need a stable way to name each end: an **object
identity** (primary key). The ontology has no such concept today — `ObjectType` is just
`{name, properties, derived, table}`, and identity exists only physically (the columns
FK links reference). So this slice first adds identity, then consumes it.

## Scope

In scope:

- **Part A — object identity.** A nullable, declared primary-key property on
  `ObjectType`, persisted in the ontology, validated when a type is bound.
- **Part B — source→target association.** A traversal read that projects the source
  and final-target identity values as deduped pairs, governed exactly like the existing
  chain read plus identity-visibility on the two projected ends.

Out of scope (a clean, fast follow-up, not this PR):

- **Object-set inputs keyed on identity** (`?ids=1,2,3` → an `in:` predicate on the
  source's declared identity column). This is sugar over the already-shipped `in:`
  set operator on the identity column, so it is a small separate slice.

## Part A — object identity

### Core

Add one field to `ObjectType` (`control-plane/core/src/ontology.rs`):

```rust
pub struct ObjectType {
    pub name: TypeName,
    pub properties: Vec<PropertyDef>,
    pub derived: Vec<DerivedPropertyDef>,
    pub table: TableRef,
    /// The property that is this type's primary key, if declared. Names one of
    /// `properties`. `None` = no declared identity (back-compatible).
    pub identity: Option<String>,
}
```

`None` keeps every existing type and call site working; identity is opt-in.

A single declared identity column (vs a per-`PropertyDef` `is_key` flag) is chosen
deliberately: it matches Foundry's single-primary-key object model, keeps "the
identity" unambiguous for association, and is one nullable column rather than a flag
threaded through every property.

### Storage (postgres adapter)

- **Migration:** a new migration file adds the column:
  `ALTER TABLE ontology.object_type ADD COLUMN identity text;` (nullable; existing rows
  get `NULL`).
- **`define_type`:** include `identity` in the `ontology.object_type` upsert (INSERT
  column + `ON CONFLICT … DO UPDATE SET identity = excluded.identity`).
- **`get_type`:** SELECT `identity` and populate the field.
- **`.sqlx`:** regenerate via `tools/sqlx-prepare.sh` and commit (the object_type
  insert/select queries change).

### Storage (memory adapter)

The memory ontology fake stores and returns `identity` with the rest of the
`ObjectType` (it already holds the whole struct; just carry the new field).

### Validation at bind (`ingest/bind.rs`)

`bind` already validates each property against the physical table schema before
`define_type`. Extend it: when `type_def.identity` is `Some(name)`,

- `name` must be one of `type_def.properties` (else a violation), and
- that property must be `required: true` — a primary key cannot be nullable (else a
  violation).

A new `BindViolationReason::BadIdentity` variant carries the reason; it is collected
into the same `Vec<BindViolation>` as the existing checks (nothing persists on
rejection). `define_type` itself trusts its input (as it already does for properties);
bind is the validation chokepoint.

### Contract

Extend the ontology contract (testkit, run on both adapters): define a type with a
declared `identity`, read it back, assert the field round-trips; define one with
`identity: None`, assert it stays `None`.

## Part B — source→target association

### Read entry point

```rust
pub struct Associations {
    pub from_id_type: String, // logical type of the source identity property
    pub to_id_type: String,   // logical type of the target identity property
    pub pairs: Vec<(SqlValue, SqlValue)>,
}

pub async fn read_associations(
    q: &ChainQuery,
    subject: &Subject,
    deps: &QueryDeps<'_>,
) -> Result<Associations, QueryError>;
```

`read_associations` resolves the chain **identically** to `read_linked_chain`:

- the same depth check, source Read gate, per-hop type resolution (forward/inverse),
  Read-on-every-reached-type guarantee, per-position row-filter loading, and caller
  `ChainFilter` coercion/visibility checks — all unchanged. The N-ends governance
  (a caller only reaches final targets through intermediate rows the policy permits)
  carries over verbatim, because the joins and WHERE are built the same way.

It then differs only in **projection and result shape**:

- **Identity required on both ends.** The source (position 0) and final target
  (position last) `ObjectType`s must each have `identity: Some(_)`. If either is `None`,
  return `QueryError::NoIdentity(type_name)` → HTTP 400. (Intermediate positions need
  no identity — they are traversal-only.)
- **Identity must be visible on both ends.** The identity property must survive each
  end's projection (not in `denied`, not in `masked`). If a projected end's identity is
  denied or masked, return `QueryError::Forbidden` — you cannot associate objects you
  cannot identify. (This reuses the existing `project_allowed` / `masked` sets already
  computed per position.)
- **Projection.** `SELECT DISTINCT t_0.<source_identity>, t_{k}.<target_identity>`
  through the same governed joins and WHERE. Deduped on the pair.

### Compiler

A `compile_chain_pairs` variant in `sql.rs` that reuses `compile_chain_with`'s
FROM/JOIN/WHERE construction (same `t_{i}` aliasing, same hop joins, same per-position
row-filter + caller-predicate conjuncts) but replaces the final-target column
projection with exactly two columns:
`SELECT DISTINCT {t_0}.{src_id}, {t_k}.{tgt_id}`. Signature takes the source identity
column name and the target identity column name instead of `allowed_cols`/`mask_cols`.
Factor the shared FROM/JOIN/WHERE assembly so the two compilers do not duplicate it.

### Output / render

`associations_to_json(&Associations)` →

```json
{ "associations": [ { "from": <typed id>, "to": <typed id> }, ... ] }
```

Each id is typed by its identity property's logical type via the existing
`render_value` machinery (so a `Long` identity renders as a JSON string, a `Double` as
a number, etc.) — `from` uses `from_id_type`, `to` uses `to_id_type`.

### HTTP

A `?shape=association` query flag on the **existing** chain routes —
`/objects/:from_type/links/:link_name` (single hop) and `/objects/:from_type/links`
(multi-hop). Absent or `shape=objects` → today's `read_linked_chain` +
`objects_to_json` (unchanged default). `shape=association` →
`read_associations` + `associations_to_json`. All existing path/direction/filter query
parsing is reused as-is; only the terminal call + renderer switch on the flag. An
unrecognized `shape` value → 400.

`QueryError::NoIdentity` maps to 400 (a client asked to associate a type without a
declared identity); the existing error→status mapping handles `Forbidden` (403),
`BadFilter`/`BadChain`/`AmbiguousLink` (400), etc. unchanged.

## Governance summary

Association inherits the chain read's full governance — Read required on the source and
every reached type, per-type row-filters AND'd into the joins, and caller filters at
any position only narrowing within already-permitted visibility — and adds exactly one
rule: the two **projected** ends (source + final target) must have a declared,
caller-visible identity. Intermediate hops are unaffected. A row-filter on any position
still removes the pairs that route through forbidden rows.

## Testing

- **Identity round-trip contract** (testkit, both adapters): `identity` persists through
  `define_type`/`get_type`; `None` stays `None`.
- **Bind accept/reject matrix** (`ingest`): identity naming a declared required property
  → accepted; identity naming an unknown property → `BadIdentity`; identity naming a
  non-required property → `BadIdentity`; `identity: None` → accepted.
- **Association e2e** (fixture + DuckDB read-back):
  - single-hop: `?shape=association` returns the exact source↔target id pairs.
  - multi-hop: pairs are (source identity, final-target identity); intermediate is
    traversal-only.
  - governance: a row-filter (or caller filter) on an intermediate/target position
    drops the pairs that route through excluded rows.
  - dedup (on the `(from, to)` pair): the *same* source reaching the *same* target via
    two distinct intermediate paths yields **one** pair (`DISTINCT` collapses it);
    *different* sources reaching one target yield distinct pairs (different `from`).
    Assert both explicitly.
  - `NoIdentity`: associating a type whose source or target lacks a declared identity
    → 400.

## Task breakdown

1. **Object identity** — `ObjectType.identity` field; ontology migration; postgres
   `define_type`/`get_type` + `.sqlx`; memory fake; `bind` validation +
   `BadIdentity`; identity round-trip contract + bind matrix tests.
2. **Source→target association** — `read_associations`, `compile_chain_pairs`,
   `Associations` + `associations_to_json`, the `?shape=association` HTTP flag and
   `NoIdentity`→400 mapping; association e2e.
3. **Docs** — mark the slice delivered in the roadmap; note object-set-by-identity
   input as the remaining deferred follow-up in `docs/FUTURE.md`.
