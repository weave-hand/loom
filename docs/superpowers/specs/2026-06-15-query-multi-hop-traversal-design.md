# Design: governed multi-hop traversal (Step 3, query — relational reads, slice C part-1)

> **Status:** approved design (2026-06-15). The third richer-reads slice, after governed link
> traversal (slice A, `2026-06-14-query-governed-link-traversal-design.md`) and derived
> properties (slice B, `2026-06-15-derived-properties-design.md`). Slice A made a single link
> **resolvable** and added a **both-ends-governed** traversal that joins across it; slice B added
> aggregate-over-link derived properties. This slice generalizes A's single-hop traversal to a
> **chain of N links** — `Customer → Order → LineItem` — with governance composed across *every*
> hop, reusing A's per-hop join primitive.

## Goal

Add a governed read that, given a **source type**, an **ordered chain of links**, and **equality
filters on the source**, returns the **deduped objects of the final target type** — with **every
hop in the chain governed**. Concretely: "from Customers in CA, follow `orders` then `lineItems`,
return the LineItems I am allowed to reach through Orders I am allowed to see." Each type in the
chain (source, every intermediate, final target) requires `Read`, and each type's row-filters are
applied inside the join, so a caller can only reach final targets through intermediate rows the
policy permits.

This is the N-hop generalization of slice A. Slice A fixed two-type governance composition once;
this slice composes it across an arbitrary (capped) chain. Single-hop traversal becomes the `N=1`
case of the same compiler.

## North star (context, not part-1)

Richer reads ultimately span derived/aggregate properties (slice B, delivered), multi-hop / object-set
traversal (this slice), and cross-type filtering. This part delivers **forward, source-filtered,
deduped multi-hop chaining**. Inverse-direction hops, caller-supplied target/intermediate filters,
object-set inputs, and returning the source→target association are explicit follow-ons.

## Decisions (settled in brainstorming)

- **A — Multi-hop chaining is the part-1 anchor.** Among slice C's bundle (multi-hop, inverse,
  target-side filtering, object-set inputs, association), part-1 delivers forward multi-hop chaining;
  the rest are later parts.
- **B — Govern every hop (leak-free).** Require `Read` on every type in the chain (source, each
  intermediate, final target); apply every type's row-filters as conjuncts inside the join. Any hop
  type `Read`-denied → `403`. Generalizes slice A's both-ends rule to N-ends and closes the multi-hop
  inference leak (reaching a final target through an intermediate row the caller may not see).
- **C — `?path=` query-param surface.** `GET /objects/{from}/links?path=l1,l2,…,lk`. The existing
  single-hop route `GET /objects/{from}/links/{link}` stays, reimplemented as a thin forward into the
  chain handler with `path=[link]`, so there is one chain compiler and single-hop is the `N=1` case.
- **D — Depth-capped, deduped, source-filtered.** Chain depth capped at a small constant (4 hops);
  the final-target set is deduped (`DISTINCT` over the visible projection, the same decision as slice
  A); equality filters apply to the **source** type only (target-side filtering is a later part).

## What this slice IS

- A new **`compile_chain`** SQL compiler in `query-api/sql.rs` that emits a chain of INNER JOINs
  (FK and join-table hops), with every hop type's row-filters AND'd into the WHERE and only the
  final target projected. Single-hop `compile_traversal` is replaced by `compile_chain` (the `N=1`
  case); the existing single-hop route forwards into the chain handler.
- A **`read_linked_chain`** handler path that resolves the chain, gates `Read` on every hop type,
  loads every hop's policy, and serves the deduped final-target objects through the existing
  typed-JSON render path.
- A **new route** `GET /objects/{from}/links?path=…` and the existing single-hop route forwarding
  into it.

## What this slice is NOT

- **No inverse-direction hops** — every hop follows a link from its declared `from` to its `to`.
  Following a link backwards is a later part.
- **No caller-supplied target or intermediate filters** — equality filters bind to the source type
  only; intermediate/target governance is row-filters (policy), not caller input.
- **No object-set inputs** — traversal starts from source equality filters, not a passed/saved set
  of object IDs.
- **No source→target association** — the result is the deduped final-target object set, not pairs
  carrying which source each target came from.
- **No define-time chain validation** — links are resolved at read time; a broken chain surfaces
  then as a `400`.
- **No derived properties on chain output** — `read_object` only (slice B); chain output is physical
  columns of the final target.

## Design

### 1. HTTP surface

- **New:** `GET /objects/{from}/links?path=l1,l2,…,lk` with `k ≥ 1`. `path` is a comma-separated,
  ordered list of link names. Source equality filters use the existing `{Type.prop}=value` query
  syntax (e.g. `&Customer.region=CA`), restricted to the **source** type `{from}`.
- **Existing:** `GET /objects/{from}/links/{link}` is retained and reimplemented to forward into the
  chain handler with `path = [link]`. No behavior change for existing callers; one compiler.
- **Depth cap:** `k > 4` → `400`. Empty/blank `path` → `400`.

### 2. Chain resolution (`handler.rs`)

Resolve the chain left-to-right, starting at the type named `{from}`:

1. `get_type(from)` — unknown → `404` (`UnknownType`, existing).
2. For each segment `li` (in order): among the **current type's** links (`ontology.links(current)`),
   find the `LinkDef` whose `name == li`. Missing, or a link whose `from` is not the current type,
   → `400` (malformed chain). Its `to` becomes the next current type; record `(link, to_type)`.
3. The chain is the ordered list `[source, t_1, …, t_k]` of types and `[link_1, …, link_k]` of links.

### 3. Governance (both-ends → N-ends)

For **every** type in the resolved chain (source, each intermediate, final target):

1. **Coarse `Read` gate:** `acl.check(subject, Read, Type(t))`. Any `Deny` → `403` (the whole
   traversal is forbidden — consistent with `read_object` on an unreadable type).
2. **Load policy:** `load_policy(acl, subject, Type(t))` → that type's `row_filters` (and, for the
   **final target only**, its `denied`/`masked` column sets for projection).

Row-filters from *every* hop type are applied inside the join (§4). Column masking/denied-columns
apply only to the **final target** (the only projected type); intermediate column policy does not
gate the traversal beyond the coarse `Read` check, because intermediate columns are never returned —
only used as join keys and row-filter predicates. (Row-filters reference trusted ontology property
names, quoted as identifiers, same as slice A's target row-filters inside the subquery.)

### 4. SQL compilation (`compile_chain`)

One query — a chain of INNER JOINs from the final target back to the source — with governance
enforced by construction:

```sql
SELECT DISTINCT <final-target allowed/masked cols>
FROM   <final_target> t_k
JOIN   <type_{k-1}>   t_{k-1} ON <hop_k join predicate>
…                                                        -- one JOIN per hop
JOIN   <source>       t_0     ON <hop_1 join predicate>  -- (join-table hop adds its mapping join)
WHERE  <source eq-filters on t_0>
  AND  <t_0 row_filters> AND <t_1 row_filters> AND … AND <t_k row_filters>
LIMIT  <DEFAULT_LIMIT>
```

- **Per-hop join predicate** reuses slice A's `LinkBacking` join shapes, aliased by position:
  - **`ForeignKey { from_column, to_column }`** for hop `i` linking `t_{i-1}` (from) to `t_i` (to):
    `t_i."<to_column>" = t_{i-1}."<from_column>"`.
  - **`JoinTable { table, from_key, from_column, to_column, to_key }`** for hop `i`: an extra join
    of the mapping table `ji` —
    `JOIN <table> ji ON ji."<from_column>" = t_{i-1}."<from_key>"` and the predicate
    `t_i."<to_key>" = ji."<to_column>"`.
- **Projection:** only the final target `t_k` is projected, reusing the single-hop projection logic
  (allowed columns; masked columns rendered as the `'***'` marker). Intermediates and the source
  contribute join keys and row-filter predicates only.
- **Governance:** every hop type's row-filters are AND'd into the WHERE — the leak-free guarantee.
  The source equality filters bind to `t_0`.
- **Injection safety:** all identifiers (table schema/name, every join column, mapping-table
  names/columns, projected/row-filter property names) flow through `quote_ident`; all values
  (source eq-filter values, row-filter values) bind as `?` params, pushed in stable left-to-right
  order (projection has no params; then JOIN predicates have no params; then WHERE: source
  eq-filters, then each hop's row-filter params in chain order).
- **Dedup:** `DISTINCT` over the visible final projection — the same dedup decision as slice A (a
  subject perceives a target only through its visible projection; deduping on a possibly-denied key
  would leak it).

The existing single-hop `compile_traversal` (slice A) is **replaced** by `compile_chain` (the `N=1`
case), keeping one compiler. The existing single-hop route forwards into the chain handler with
`path=[link]`. Slice A's single-hop unit tests are **updated** to assert the `compile_chain` `N=1`
output — the join/projection/dedup semantics are identical; the only textual change is positional
aliasing (`t_0`/`t_1`, and `j1` for a join-table hop) in place of slice A's `f`/`t`/`j`. The
single-hop governed e2e (`link_traversal.rs`) is retained **unchanged** and proves single-hop
behavior is preserved through the new compiler (it asserts JSON results, not SQL text).

### 5. Error handling

- **Malformed chain** (unknown link name, a link whose `from` is not the current type, empty
  `path`, depth > cap) → `400`. These are bad request shapes, not authorization failures, so they
  are reported as such (not silently emptied).
- **Read-denied** on any hop type → `403` (coarse, like `read_object`).
- **Final-target all-columns-denied** → empty projection → handled exactly as the single-hop path
  handles it today.
- **Unknown source type** → `404` (`UnknownType`, existing).

### File structure

- **query-api:** `compile_chain` + per-hop predicate helper in `sql.rs`; chain resolution +
  N-ends governance in `handler.rs`; the new `?path=` route + the existing route forwarding in the
  HTTP layer (router/service module). Unit tests in `tests/sql_compile.rs`; a governed e2e in
  `tests/multi_hop_traversal_e2e.rs`.
- **Docs:** roadmap delivered marker (slice C part-1); `docs/FUTURE.md` follow-ups.

## Testing

- **Unit (`sql_compile.rs`):**
  - 2-hop FK chain compiles to the expected two-JOIN `SELECT DISTINCT … LIMIT` with the source
    eq-filter and both intermediate+target row-filters AND'd in, params in chain order.
  - 2-hop chain mixing an FK hop and a join-table hop (the mapping-table join appears for the
    join-table hop only).
  - Param-order assertion: source eq-filter param precedes hop row-filter params, in chain order.
  - `N=1` chain reproduces single-hop traversal semantics; slice A's single-hop unit tests are
    updated to the chain aliasing (`t_0`/`t_1`) and stay green.
- **Governed e2e (`multi_hop_traversal_e2e.rs`, `loom_fixture_test`, hermetic PG + DuckDB):** seed
  `customer → orders → line_items` (an FK chain). Prove:
  - **(a) served + correct:** a fully-permitted subject gets exactly the deduped LineItems reachable
    from the filtered Customers.
  - **(b) mid-hop governance bites:** an intermediate `Order` row-filter (`status='shipped'`)
    narrows the reachable LineItems to those through shipped Orders (distinct from the unfiltered
    result).
  - **(c) intermediate Read denied → 403:** a subject with `Read` on Customer + LineItem but **not**
    Order is forbidden (cannot traverse through a type it cannot read).

## Decisions

- **A** multi-hop chaining is the part-1 anchor; **B** govern every hop (leak-free, N-ends);
  **C** `?path=` query-param surface with the existing single-hop route forwarding in; **D**
  depth-capped (4), deduped (`DISTINCT` over visible projection), source-filter only.

## Follow-ups (later parts of slice C / richer reads)

- **Inverse-direction hops** — follow a link from its `to` back to its `from` within a chain.
- **Caller-supplied target / intermediate filters** — equality filters on types beyond the source
  (target-side filtering).
- **Object-set inputs** — start a chain from a passed/saved set of source object IDs instead of
  source equality filters.
- **Source→target association** — return which source each final target came from (pairs), not just
  the deduped target set; ties into a visible-primary-key concept.
- **Define-time chain/link validation** — validate link continuity and physical columns at authoring
  time (shared with slice A's deferred `define_link` column validation).
- **Derived properties on chain output** — serve slice B's aggregates on traversal/chain output.

## Roadmap

Lands under Step 3 → Query, the richer-reads track, **slice C part-1 (multi-hop traversal)** —
building directly on slice A's resolvable-link + governed-join primitive: a chain is that join,
repeated per hop with every hop's governance composed in.
