# Inverse-Direction Link Hops — Design

**Slice:** Slice-C part-3 of the governed traversal arc. Today traversal is forward
only (`source --link--> target`). This adds the inverse direction
(`target <--link-- source`), governed at every hop, across both the single-hop
(`/objects/{type}/links/{link}`) and multi-hop (`/objects/{type}/links?path=...`)
read paths, with per-hop direction in chains.

## Motivation

Links are directed and stored on the `from` side: `Order.placed_by -> Customer`.
Today the only question loom can answer is "given an Order, who is its Customer?"
The inverse question — "given a Customer, which Orders point at them?" — is
unanswerable, because the starting type (`Customer`) is the link's `to` side and the
link is indexed only by its `from`. Inverse hops close that gap: every link becomes
traversable in both directions, governed identically.

## Core insight

The multi-hop SQL compiler (`query-api/src/sql.rs::compile_chain_with`) already joins
**symmetrically**: for a hop connecting `types[i-1]` (from) to `types[i]` (to) it emits
`from_alias.from_column = to_alias.to_column`. An inverse hop therefore needs **no
compiler change**. It needs exactly two things:

1. An **inbound** link lookup — links are stored on the `from` side only, so an inbound
   traversal (whose starting type is the link's `to`) cannot find them today.
2. **Reversing the link backing's column roles** before handing it to the compiler, and
   following `link.from` instead of `link.to`.

## New control-plane primitives (`control-plane-core`)

### `LinkBacking::reversed(&self) -> LinkBacking`

A pure column-role swap that turns a backing for `A --l--> B` into the backing the
compiler needs to join `B` to `A`:

- `ForeignKey { from_column, to_column }`
  → `ForeignKey { from_column: to_column, to_column: from_column }`
- `JoinTable { table, from_key, from_column, to_column, to_key }`
  → `JoinTable { table, from_key: to_key, from_column: to_column, to_column: from_column, to_key: from_key }`

`reversed()` is an involution: `b.reversed().reversed() == b`. The mapping table
(`table`) is unchanged. Cardinality is **not** part of `LinkBacking` and is not reversed
here (see Cardinality below).

**Correctness (FK).** Forward link `A.l -> B`, `FK{from_column: fk (on A), to_column: pk
(on B)}`. Inverse hop at chain position `i` connects `types[i-1] = B` to `types[i] = A`.
The compiler emits `t_{i-1}.from_column = t_i.to_column`. With `reversed() =
FK{from_column: pk (on B), to_column: fk (on A)}` that is `B.pk = A.fk` — correct.

**Correctness (JoinTable).** The compiler emits, for a JoinTable hop,
`JOIN jt j ON j.to_column = to_alias.to_key JOIN from_tbl from_alias ON
from_alias.from_key = j.from_column`. For the inverse hop `B` (from) to `A` (to),
`reversed() = JoinTable{from_key: to_key (on B), from_column: to_column (jt col for B),
to_column: from_column (jt col for A), to_key: from_key (on A)}` yields
`j.<jt-col-for-A> = A.<from_key on A>` and `B.<to_key on B> = j.<jt-col-for-B>` — correct.

### `Ontology::links_to(&self, name: &TypeName, page: PageReq) -> Result<Page<LinkDef>>`

The inbound adjacency query — the mirror of the existing `links` (outbound). Returns all
links whose `to` is `name`. `NotFound` if the type itself is absent, exactly like `links`.

- **Memory adapter:** filter the stored links by `l.to == *name` (after the type-exists
  check), mirroring the existing `links` impl which filters by `l.from`.
- **Postgres adapter:** the existing `links` query with `where to_type = $1` instead of
  `where from_type = $1`; reuses `backing_from_row`/`cardinality_from_str`. A new committed
  `.sqlx` entry covers it.
- **testkit contract:** an inbound-links round-trip exercising both adapters (define a
  link, assert `links_to(to)` returns it and `links_to(from)` does not).

## Request encoding

- **Single-hop:** `GET /objects/{type}/links/{link}?direction=inverse`. The `direction`
  query parameter defaults to `forward`; any value other than `forward`/`inverse` is a
  **400**.
- **Multi-hop:** `GET /objects/{type}/links?path=~placed_by,ships_to`. A `~` prefix on a
  path element marks that hop **inverse**; a bare element is **forward**. `~` is an
  RFC-3986 *unreserved* character, so it needs no URL-encoding.

Direction is parsed **at the HTTP edge** into a structured internal representation; no
sigils leak into the handler core.

## Internal types (`query-api/src/handler.rs`)

```
enum Direction { Forward, Inverse }   // Default = Forward
struct Hop { link: String, direction: Direction }
impl From<&str> for Hop / From<String> for Hop  // -> Forward hop
```

- `ChainQuery.path` becomes `Vec<Hop>` (was `Vec<String>`). The `From<&str>`/`From<String>`
  conversions keep every existing forward call site (`path: vec!["orders".into(), ...]`)
  compiling unchanged — only direction-aware code constructs `Hop` explicitly.
- `LinkQuery` stays **forward-only** (its three fields unchanged); `read_linked_objects`
  delegates to `read_linked_chain` with a single forward `Hop`. It remains a convenience
  wrapper — no existing single-hop call site changes.
- **Single-hop direction is delivered at the HTTP edge:** `get_linked` parses `?direction=`
  and builds a one-element `ChainQuery { path: vec![Hop { link, direction }] }`, calling
  `read_linked_chain` directly. `read_linked_chain` is the single governed traversal entry;
  the single-hop case is just a one-`Hop` path with no special core code.
- `read_linked_chain` per-hop resolution branches on `hop.direction`:
  - **Forward:** `ontology.links(current)` → find by name → next type = `link.to`, push
    `link.backing` as-is. *(today's behavior)*
  - **Inverse:** `ontology.links_to(current)` → find by name → next type = `link.from`,
    push `link.backing.reversed()`.
  - Both branches then Read-gate and row-filter the next type identically — every type the
    chain touches stays both-ends governed, with no new governance code.

`chain_filter::resolve_chain_filters` is fed the **bare** link names (sigils stripped), so
it is **unchanged**. Filter keys reference bare link names; a link repeated in the path
(even with mixed direction, e.g. `path=placed_by,~placed_by`) keeps its existing
`AmbiguousLink` treatment — the `/graph` boundary.

## Error handling

- Inbound link name not found in `links_to(current)` → `QueryError::UnknownLink` (**404**),
  the same as a missing forward link.
- **Multiple** inbound links share the requested name — links are keyed by `(name, from)`,
  so `(name, to)` is not unique (e.g. both `Order.placed_by -> Customer` and
  `Refund.placed_by -> Customer`) — → new `QueryError::AmbiguousLink(String)` → **400**. A
  deterministic governed read cannot silently pick one. This mirrors the existing
  `chain_filter::FilterResolveError::AmbiguousLink` precedent.
- An unknown/invalid `direction` value on the single-hop path → **400** (handled at the
  HTTP edge before the core runs).

## Cardinality

A forward `Order.placed_by -> Customer` link is many-to-one; its inverse (a customer's
orders) is one-to-many. The traversal returns a **set** either way, and the existing
`SELECT DISTINCT` in `compile_chain_with` already collapses duplicates. Cardinality is not
consulted by the compiler and needs no special handling in this slice.

## Files touched

| File | Change |
|------|--------|
| `control-plane/core/src/ontology.rs` | `LinkBacking::reversed()`; `Ontology::links_to` trait method |
| `control-plane/memory/src/ontology.rs` | `links_to` impl (filter `l.to == name`) |
| `control-plane/postgres/src/ontology.rs` (+ `.sqlx`) | `links_to` impl (`where to_type = $1`) |
| `control-plane/testkit/src/lib.rs` | inbound-links round-trip contract (both adapters) |
| `query-api/src/handler.rs` | `Direction`/`Hop`, per-hop direction resolution, `AmbiguousLink` |
| `query-api/src/http.rs` | parse `?direction=` + `~` path sigils; map `AmbiguousLink` → 400 |
| `query-api/src/sql.rs` | no logic change (doc note that the compiler is direction-agnostic) |

## Testing

- **core:** `LinkBacking::reversed()` unit test — FK and JoinTable, asserting the exact
  swapped fields and the double-reverse involution.
- **testkit contract:** inbound-links round-trip across both adapters.
- **sql_compile:** inverse single-hop and a mixed-direction chain — assert the reversed
  JOIN columns appear in the emitted SQL.
- **e2e (fixture):** customer → their orders (inverse single-hop served); a mixed-direction
  chain; governance denied on the inbound hop's source type → Forbidden; ambiguous inbound
  → 400; unknown inbound link → 404.
- **http parse:** `~` sigil parsing and `?direction=` parsing (incl. the invalid-value
  400), as pure unit tests on the edge helpers.

## Out of scope

- A `/graph` surface for cyclic/self-link/repeated-link per-hop filtering (already the
  deferred boundary; unchanged here).
- Object-set inputs and the source→target association (the remaining slice-C parts).
- Reversing cardinality metadata or exposing an inverse `LinkDef` view — only the physical
  backing is reversed, which is all the read path needs.

## Decisions

- **Inbound ambiguity is a 400, not a silent pick.** `(name, to)` is not unique; a governed
  read must be deterministic.
- **`~` (not `<`/`-`) is the inverse sigil** — it is RFC-3986 unreserved, so no
  URL-encoding is required in a query string.
- **`reversed()` lives on `LinkBacking` in core**, not as a query-api helper: reversing a
  link's physical join is a fundamental ontology operation and is unit-testable with core's
  existing harness.
