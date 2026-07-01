# `/graph` part-5: inverse hops inside a path-cycle — design (2026-07-01)

> Query-pillar slice, continuing the graph arc. Where part-2
> (`2026-06-18-graph-path-cycle-design.md`) repeats a **cyclic path** whose every hop is
> followed **forward**, this slice lets a hop be followed **backward** — so one `/graph`
> traversal can alternate edge direction mid-path (e.g. `memberOf,~memberOf`). Builds
> directly on part-2's cyclic-path machinery and reuses the inverse-hop primitives already
> shipped for the relational `/links` chain (`Hop`/`Direction`/`LinkBacking::reversed()`).
> `road-inverse-in-path` (area: query).

## Problem

Part-2 resolves a `?path=l1,…,lK` cycle by walking each link **forward** from the queried
type; a cycle is formed only when forward links happen to return to the start type. That
forces the ontology to declare the return edge as its own link. The canonical
shared-membership query `Person --memberOf--> Team --hasMember--> Person` needs **both**
`memberOf` (Person→Team) and `hasMember` (Team→Person) defined, even though `hasMember` is
nothing more than `memberOf` traversed backward through the same join table. Any cycle
that "goes out and comes back the same way" is unexpressible without a redundant inverse
link.

The relational `/links` chain already solved exactly this for fixed-length traversal: a
path element prefixed with `~` is followed backward, resolved via inbound adjacency
(`Ontology::links_to`) with the backing's column roles swapped (`LinkBacking::reversed()`).
`ChainQuery.path` is a `Vec<Hop>`; `resolve_chain` has a `Direction::Forward` /
`Direction::Inverse` branch; `parse_path_hops` parses the `~` sigil; `inverse_hops_e2e.rs`
proves it end to end. The `/graph` path-cycle is the one traversal surface that has **not**
inherited this — `GraphQuery.path` is still a `Vec<String>` (forward-only). This slice
closes that gap: a `/graph` path may mix forward and backward hops, so `memberOf,~memberOf`
answers the shared-membership question with only `memberOf` declared.

## Approach

Lift the already-shipped relational-chain inverse machinery into the `/graph` path-cycle,
changing **nothing** about how a hop's SQL is emitted. The key observation — the same one
that made `chain_from_where` and `link_join` "direction-agnostic" (sql.rs:489) — is that an
inverse hop is fully expressed by two substitutions made at **resolution** time, before the
compiler ever runs:

1. the hop lands on the link's **origin** type (`link.from`) instead of its target
   (`link.to`), resolved by inbound adjacency (`links_to`) instead of outbound (`links`);
2. the hop's backing is `link.backing.reversed()` — the column roles swapped so the
   symmetric equijoin reaches back to the origin.

`compile_graph_reach` consumes only `GraphStep { backing, next_table, next_filters }` and
joins each step through the shared `link_join` helper (a symmetric
`from_alias.from_column = to_alias.to_column`). Because a `reversed()` backing already
encodes the backward join, **the compiler is unchanged** — an inverse hop is just a
`GraphStep` whose `backing` was reversed and whose `next_table` is the origin type's table.
All the change lives in the type of `GraphQuery.path`, the path-cycle resolution loop in
`read_graph_reach`, and the HTTP path parse. This mirrors part-2's own claim that a 1-step
path is byte-identical to part-1 — here, a forward-only path is byte-identical to today.

No new compiler, no new SQL shape, no new recursive-CTE structure. That is deliberate: the
cyclic-path CTE (seed anchor, `UNION` dedup, `r.depth < N` bound, per-step intermediate
governance) is exactly what we want to reuse, and direction is orthogonal to it.

## Grammar extension

The path grammar already has the sigil defined and parsed — this slice **routes the
existing parser** into the graph path instead of re-splitting the string.

- **Sigil.** A `?path=` element whose first non-whitespace character is `~` is an **inverse
  hop**; the `~` is stripped and the remainder (re-trimmed) is the link name. A bare element
  is a forward hop. This is `INVERSE_SIGIL` in `path_parse.rs`, already used by the `/links`
  chain's `_path` param.
- **Why `~`.** It is RFC-3986 *unreserved*, so it never needs URL-encoding in a query
  string, and it is not a legal identifier character, so it cannot collide with a link name.
  `parse_path_hops` trims around it (`~ memberOf` == `~memberOf`) and drops empty elements,
  so `memberOf,,~memberOf` and trailing commas are tolerated exactly as for the chain.
- **Change.** `get_graph_path` (http.rs) currently splits `?path=` into a `Vec<String>` by
  hand (comma-split, drop-empty). Replace that with `parse_path_hops(&v)` → `Vec<Hop>`, and
  thread `Vec<Hop>` through `graph_respond` into `GraphQuery.path`. The `?links=` (part-3
  union) and `*` (part-B recursive-core) branches are untouched by this substitution (see
  Composition / Non-goals for their interaction with `~`).
- **Ambiguity / escaping concern.** A link literally named with a leading `~` is
  unaddressable — the same limitation the chain already accepts; link names are ordinary
  identifiers and do not begin with `~`, so there is no escape mechanism and none is added.
  `~` and `*` are distinct sigils at opposite ends of an element (`~` is a **prefix**
  direction marker; `*` a **suffix** recursive-core marker); combining them is out of scope
  (Non-goals), and the parser treats a `~…*` element as an inverse hop whose name ends in
  `*`, which fails link resolution as an `UnknownLink` — an acceptable, non-silent outcome.

## Traversal semantics (forward vs inverse JOIN)

A path is resolved into an ordered `Vec<GraphStep>` by walking hops from the queried type,
tracking a `current` type. Per hop:

- **Forward `l`** (unchanged from part-2): resolve `l` in `links(current)` (outbound). The
  hop lands on `link.to`; its backing is `link.backing` verbatim. For a link
  `A --l--> B` backed by `ForeignKey { from_column, to_column }`, the emitted join is
  `A.from_column = B.to_column`; for `JoinTable { from_key, from_column, to_column, to_key }`
  it is the two-join form `A.from_key = jt.from_column AND jt.to_column = B.to_key`.
- **Inverse `~l`**: resolve `l` in `links_to(current)` (inbound adjacency — links whose `to`
  is `current`). The hop lands on `link.from`; its backing is `link.backing.reversed()`.
  `reversed()` (ontology.rs) swaps the column roles — FK `{from_column, to_column}` becomes
  `{to_column, from_column}`; a `JoinTable` swaps `from_key↔to_key` and
  `from_column↔to_column`, leaving the mapping table itself unchanged. So for the same link
  `A --l--> B`, the inverse hop from `B` emits `B.to_column = A.from_column` (FK) or the
  mirror join-table form — reaching `A` through the identical physical relation, no second
  link required. `reversed()` is an involution (`b.reversed().reversed() == b`), so a
  round-trip forward-then-inverse over one link is well-defined.

Worked example — `GET /objects/Person/graph?path=memberOf,~memberOf` where `memberOf` is a
join-table link `Person --memberOf--> Team` over `membership(person_id, team_id)`:

- hop 1 forward `memberOf`: `current` Person → Team, backing = the join-table backing.
- hop 2 inverse `~memberOf`: `links_to(Team)` finds `memberOf` (its `to` is Team); `current`
  Team → Person, backing = `membership` backing **reversed** (keys/columns swapped).
- after hop 2 `current == Person` ⇒ a valid cycle. Each recursive application walks
  Person→Team→Person: "Persons who share a team", identical to part-2's
  `memberOf,hasMember` but with **only** `memberOf` declared.

The compiler receives `[GraphStep{ backing: membership, next_table: team, next_filters:
[team-filters] }, GraphStep{ backing: membership.reversed(), next_table: person,
next_filters: [] }]` and emits part-2's exact multi-join recursive term — the second step's
reversed backing is the only difference from a fully-forward two-step path, and it is
invisible to `reach_joins`.

## Composition with the cyclic-path machinery

Direction is orthogonal to the recursion, so every part-2 invariant carries over untouched:

- **Cyclic validation** is unchanged in *form*: after walking all K directed hops,
  `current` must equal the queried type, else `NotCyclicPath`. Direction only changes how
  `current` advances at each hop (`link.to` forward vs `link.from` inverse); the closure
  check is the same equality. Forward-then-inverse over one link is the smallest mixed
  cycle; longer alternations (`~a,b,~c,…`) validate the same way.
- **Recursive CTE / cycle detection.** The emitted SQL is part-2's `WITH RECURSIVE
  reach(id, depth)` verbatim: seed anchor, `UNION` (dedups identical `(id, depth)` rows),
  inlined `r.depth < N` termination bound, outer `SELECT DISTINCT … WHERE id IN (SELECT id
  FROM reach WHERE depth >= 1)`. Termination and dedup depend only on the depth bound and
  the identity key — **not** on hop direction — so mixing forward and backward hops cannot
  weaken cycle safety. A reversed backing is baked into `GraphStep.backing` at resolution
  time; the recursive term references it exactly as it references a forward backing.
- **Governance** (per-step) is unchanged: `Read` on the queried type and **every reached
  intermediate type** (whether reached forward via `link.to` or inverse via `link.from`),
  the intermediate's row-filters at its alias `g_i`, the start type's row-filters at the
  seed `s`, the final landing `nxt`, and the projection `p`. An inverse hop lands on a real
  type that is Read-gated and row-filtered identically to a forward landing — the leak-free
  N-ends guarantee holds regardless of direction (this is why `resolve_chain` already
  Read-gates "every reached type, forward or inverse").
- **Param order** is unchanged — it is SQL-emission order, and the SQL shape is identical.

## Governance summary

`Read` on the queried type and every intermediate type in the cycle; the start type's
row-filters at the seed, the final landed node, and the projection; each intermediate
type's row-filters in the recursive join. Identity need only be *declared* (the dedup key),
not visible. Inverse hops add no new governance surface: an inbound-resolved landing type is
gated and filtered exactly as an outbound one.

## Does an inverse hop require a declared inverse link?

**No — any link is reversible by construction; no ontology change is needed, and none is
made.** Justification:

- A link's `LinkBacking` fully determines its physical join. `reversed()` mechanically
  swaps the column/key roles to produce the backward join; nothing about the reverse
  direction is unknowable from the forward declaration. (Requiring a declared inverse would
  be redundant state that could drift from the forward link.)
- Inbound adjacency is already a first-class ontology query: `Ontology::links_to(name)`
  returns "all links whose `to` is `name`" — the exact set an inverse hop resolves against
  — and is implemented across the `memory`, `postgres`, and `testkit` backends. The graph
  resolver reuses it; no schema, migration, or `.sqlx` change.
- This matches the relational chain's shipped design decision. Making `/graph` require a
  declared inverse while `/links` does not would be a gratuitous inconsistency.

The one wrinkle inbound resolution introduces — already handled by the chain — is that a
link **name** need not be unique across inbound edges (two different source types could each
declare a link named `owns` targeting `current`). `links_to(current).filter(name == l)`
can therefore match more than one; the resolver returns `QueryError::AmbiguousLink(l)`
(400) when it does, exactly as `resolve_chain` does. Forward resolution has no such
ambiguity (`(name, from)` is the link key), so this only arises on inverse hops.

## Error handling

- **Malformed path.** Empty `?path=` (or all-empty elements) → 400 (`NotCyclicPath("")`),
  unchanged. Depth out of `1..=MAX_GRAPH_DEPTH` → 400 at the HTTP edge, unchanged.
- **Unknown link.** A forward name absent from `links(current)`, or an inverse name absent
  from `links_to(current)` → `QueryError::UnknownLink(l)` (404), unchanged.
- **Ambiguous inverse link.** An inverse name matching >1 inbound link →
  `QueryError::AmbiguousLink(l)` (400). This variant exists and is HTTP-mapped for the chain
  (`chain_error`, http.rs:359) but is **not yet** in `graph_error`; this slice adds the
  `AmbiguousLink → 400` arm to `graph_error`.
- **Non-cyclic path.** After all hops, `current != queried type` → `NotCyclicPath` (400),
  rendered with the path re-serialized (forward names bare, inverse names `~`-prefixed) so
  the message round-trips the request. (`GraphQuery.path` is now `Vec<Hop>`, so the
  `q.path.join(",")` render becomes a small hop→string helper that re-emits the sigil.)
- **Forbidden intermediate.** `Read`-denied reached type (forward or inverse) → `Forbidden`
  (403), unchanged.
- All other arms of `graph_error` are unchanged (`UnknownType`/`UnknownLink` → 404,
  `NoIdentity`/`BadFilter` → 400, opaque 500).

## Testing

loom tests are `rust_test` **integration** targets (no inline `#[cfg(test)]`); each new
target is a sibling `tests/<name>.rs` wired in `src/services/query-api/BUCK` (mirror an
existing graph target), and e2e targets use `loom_fixture_test`. Reuse
`//src/services/query-api:e2e-support` (the `get`/`subject_with_role`/`grant_read`/`ids`
helpers and the `InProcessServingEngine` router driver) and mirror the shapes in
`graph_path_e2e.rs` and `inverse_hops_e2e.rs`.

- **Path parse** (`tests/path_parse.rs`, extend): assert `parse_path_hops("memberOf,~memberOf")`
  yields `[Forward memberOf, Inverse memberOf]`, and that whitespace/empty handling around
  `~` matches the chain's cases. (The parser is shared, so this pins that `/graph` inherits
  identical grammar.)
- **Handler** (`tests/graph_reach.rs`, extend / new `tests/graph_reach_inverse.rs`): with a
  stub ontology/serving —
  - a mixed path `memberOf,~memberOf` on Person resolves to a valid cycle (steps carry the
    reversed backing on the second step; `next_table` is the origin table);
  - an inverse hop whose name is absent inbound → `UnknownLink`;
  - an inverse hop matching two inbound links → `AmbiguousLink`;
  - a mixed path that does **not** close on the queried type → `NotCyclicPath`, message
    re-serialized with the `~` sigil;
  - a Read-denied inverse-landed intermediate → `Forbidden`.
- **e2e** (`tests/graph_inverse_e2e.rs`, `loom_fixture_test`, real HTTP router + in-process
  Iceberg/DataFusion serving): reuse `graph_path_e2e.rs`'s `person` + `team` +
  `membership(person_id, team_id)` graph, but declare **only `memberOf`** (Person→Team) —
  drop `hasMember`.
  - `?path=memberOf,~memberOf&depth=K` returns the same shared-team Persons that part-2's
    `memberOf,hasMember` returns, proving the inverse hop needs no declared return link;
    distinct depths differ (person 5 bridges T2/T3 as in the part-2 fixture).
  - **a cycle terminates and the node set is deduped** (the pattern inherently revisits the
    seed; assert the result is finite and distinct) — the load-bearing composition test.
  - a Read row-filter on the intermediate `Team` (e.g. `active=true`) prunes Persons
    reachable only through the inactive team — intermediate governance holds under inverse
    resolution.
  - `?path=~memberOf` alone (single inverse hop, lands on Person from Team… i.e. non-cyclic
    on Person) → 400 (`NotCyclicPath`); an ambiguous inverse link → 400 (`AmbiguousLink`);
    an unknown inverse link → 404.

## Non-goals

- **Inverse hops in the union axis (`?links=`, part-3).** Each named link there must be a
  self-link on the queried type; reversing an asymmetric self-link is meaningful but out of
  scope this slice. Deferred to `docs/FUTURE.md`.
- **Inverse hops in the recursive-core / relational-tail (`*`, part-B).** Part-B's core is a
  forward self-link and its tail is forward-only (that spec defers inverse tails
  explicitly); combining `~` with `*` on one element is unsupported and rejected as an
  `UnknownLink` (name ends in `*`), not silently.
- **A declared `inverse` field on `LinkDef`.** Reversibility is by construction (see above);
  no ontology change.
- **Named / typed backward edges, min-depth annotation, shortest-path / `/tree`, weighted
  edges** — separate follow-ons, unchanged from the part-2/3/B non-goals.

## Open questions

1. **Single-hop `/graph/:link` inverse.** The legacy part-1 route `GET
   /objects/:type/graph/:link` forwards a 1-element forward path and has its own
   `?direction=` parse (`parse_direction`, forward|inverse). Should this slice also honor
   `?direction=inverse` there for symmetry, or leave `~` in `?path=` as the sole graph
   inverse surface? (Leaning: wire `?direction=` through the same `Hop` so the two routes
   stay consistent — it is nearly free once `GraphQuery.path` is `Vec<Hop>`.)
2. **`NotCyclicPath` message fidelity.** Re-serializing `Vec<Hop>` for the error message
   (re-emitting `~`) is cosmetic; is round-tripping the exact request string worth the small
   helper, or is a plain names-only render acceptable?
3. **Ambiguity ergonomics.** `AmbiguousLink` forces the caller to disambiguate by picking a
   differently-named inbound link, but the graph path has no `from`-qualified syntax
   (`~Type.link`). Is a qualified inverse syntax worth a future slice, or is name-uniqueness
   across a type's inbound edges an acceptable expectation? (The chain lives with the same
   limitation today.)

## Files

- Modify: `src/services/query-api/src/handler.rs` — `GraphQuery.path: Vec<String>` →
  `Vec<Hop>`; add the `Direction::Inverse` branch to the path-cycle resolution loop in
  `read_graph_reach` (`links_to` + `reversed()` + `AmbiguousLink`, mirroring
  `resolve_chain`); re-serialize the path for `NotCyclicPath`.
- Modify: `src/services/query-api/src/http.rs` — `get_graph_path` parses `?path=` via
  `parse_path_hops` → `Vec<Hop>`; `graph_respond` takes `Vec<Hop>`; add `AmbiguousLink →
  400` to `graph_error`.
- Unchanged: `src/services/query-api/src/sql.rs` (`compile_graph_reach` / `reach_joins` /
  `link_join` are direction-agnostic); `path_parse.rs`; `ontology.rs`
  (`reversed()`/`links_to` already exist).
- Create: `tests/graph_inverse_e2e.rs` (and handler-level assertions); extend
  `tests/path_parse.rs`, `tests/graph_reach.rs`; wire targets in
  `src/services/query-api/BUCK` (e2e via `loom_fixture_test`).
- Docs: `docs/ROADMAP.md` (`road-inverse-in-path` → done) and `docs/FUTURE.md` (record the
  deferred inverse-in-union / inverse-in-tail follow-ons).

## Task breakdown

1. **Handler** — `GraphQuery.path: Vec<Hop>`; inverse branch in the path-cycle resolver
   (`links_to`/`reversed()`/`AmbiguousLink`); `NotCyclicPath` path re-serialization; handler
   tests (mixed cycle, unknown/ambiguous inverse, non-cyclic mix, forbidden inverse landing).
2. **HTTP** — route `?path=` through `parse_path_hops`; thread `Vec<Hop>` via `graph_respond`;
   add `AmbiguousLink → 400` to `graph_error`; extend the path-parse test.
3. **e2e** — `graph_inverse_e2e.rs` over the `memberOf`-only membership graph (equivalence to
   part-2's `memberOf,hasMember`, cycle termination + dedup, intermediate row-filter prune,
   ambiguous/unknown/non-cyclic error cases).
4. **Docs** — `docs/ROADMAP.md` delivered marker; `docs/FUTURE.md` inverse follow-ons.
