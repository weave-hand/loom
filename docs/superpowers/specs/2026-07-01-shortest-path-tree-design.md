# `/graph` shortest-path tree — design (2026-07-01)

> Query-pillar slice, continuing the graph arc. Every prior `/graph` slice
> (`2026-06-18-graph-reachability-design.md` part-1, `2026-06-18-graph-path-cycle-design.md`
> part-2, `2026-06-19-graph-recursive-core-relational-tail-design.md` part B) returns the
> reachable **set** — membership, not route. This slice serves the **path itself**: a
> shortest-path tree rooted at the seed set, so a caller can render the route/hierarchy.
> Promotes [[road-shortest-path-tree]] / [[fut-shortest-path-tree]].

## Problem

`/graph` today answers "*which* `:type` objects are reachable from the seed within N
hops?" — a deduped `{objects:[…]}` set (`read_graph_reach`, the `WITH RECURSIVE
reach(id, depth)` CTE in `sql.rs`). It throws away *how* each node was reached. A caller
that wants to draw the org chart, render the route from X to a target, or lay out a
hierarchy cannot: the set says a node is reachable but not via which parent, at what
depth, along which shortest path. Reconstructing that client-side means re-querying edge
by edge — exactly the N-round-trip shape the recursive CTE exists to avoid.

The reachable set is computed by a breadth-first `WITH RECURSIVE` expansion. BFS already
*knows* the predecessor and the (unweighted) shortest depth of every node it settles — we
simply discard it. This slice carries that information out.

## Scope / slicing

The register item names three things: shortest-path, spanning-tree, hierarchical views.
**Slice 1 delivers one primitive that subsumes all three:** a **shortest-path tree** —
for every reachable node, the predecessor edge (parent pointer) and its BFS depth, rooted
at the seed set. That single structure serves each named view without extra surfaces:

- **shortest-path-to-a-target** — the client walks parent pointers backward from the
  target node to a root; the chain *is* the shortest route. No target parameter needed.
- **hierarchical / tree view** — parent pointers *are* the hierarchy; the client folds
  them into a tree (or forest, for multiple seeds).
- **spanning tree** — an unweighted BFS tree over the reachable subgraph is a
  shortest-path spanning tree of that component; slice 1 is exactly that.

**In slice 1:**

- The shortest-path tree over a single **self-link** (`/graph/:link`) and over a
  repeated **path-cycle** (`/graph?path=…`) — the two routes that already share
  `read_graph_reach`. Selected by a response-shaping flag on the existing routes
  (see *Response shape*); the traversal machinery, ACL, and path resolution are reused
  verbatim.
- Parent pointer + BFS depth per node, deterministic tie-break, roots included.

**Deferred (recorded in `docs/FUTURE.md`):**

- **Weighted edges** ([[fut-weighted-edges]]) — BFS gives *unweighted* shortest paths
  (fewest hops). Edge weights would require Dijkstra/Bellman-Ford semantics, a weight
  column on the link, and a min-cost (not min-depth) settle rule — a materially different
  compiler. Kept deferred: loom links carry no weight today, and the unweighted tree is
  the load-bearing 80%.
- **Tree over the union route** (`?links=`, part-3) and **over the recursive-core +
  relational-tail** (part B). These have a different reachable-set shape (union of
  self-links; a tail landing on a *different* type `F` that has no single predecessor
  edge into itself). Slice 1 stays on the two routes whose reachable node *is* the
  queried type. Follow-ons.
- **Materialized full paths** in the response (each node carrying its whole root→node id
  list). Parent pointers are strictly more compact and reconstruct the same information;
  a `?paths=full` opt-in is a later convenience, not a capability.
- Weighted / k-shortest / all-shortest-paths enumeration.

Note: the tree's per-node `depth` incidentally delivers
[[fut-min-depth-annotation]] (min-depth annotation on reachable objects) — the shortest
depth is exactly the BFS settle depth. That item can close alongside this slice.

## Approach

BFS over the permitted subgraph already settles each node at its minimum depth. Extend the
recursive CTE to **carry the predecessor node id** alongside `depth`, so every emitted
`reach` row records *which* node it was reached from. The recursion enumerates candidate
`(id, depth, pred)` reach-edges (still depth-bounded, still cycle-safe); a second,
non-recursive step **settles each node to one parent** — the one on a shortest path, with
a deterministic tie-break — and the outer `SELECT` joins those settled ids back to the
table to project the object plus its `depth` and `parent`.

One SQL statement, one `fetch_rows` round-trip, governance threaded exactly as
reachability (row-filters at seed / every recursive hop / projection). The tree is a pure
extension of `compile_graph_reach`; no new traversal semantics.

## CTE change — predecessor + depth + deterministic tie-break

Reachability's CTE (`reach(id, depth)`) becomes `reach(id, depth, pred)`:

```sql
WITH RECURSIVE reach(id, depth, pred) AS (
  -- seed anchor: roots have no predecessor
  SELECT s.<id> AS id, 0 AS depth, CAST(NULL AS <id_sql_type>) AS pred
  FROM <table> s WHERE <seed predicates + row-filters@s>
  UNION
  -- recursive edge: pred is the node we expanded FROM (r.id)
  SELECT nxt.<id> AS id, r.depth + 1 AS depth, r.id AS pred
  FROM reach r JOIN <table> cur ON cur.<id> = r.id
    <path joins to nxt>
  WHERE r.depth < <N> AND <intermediate + start row-filters>
),
-- settle each node to ONE shortest-path parent, deterministically
settled AS (
  SELECT id, depth, pred,
         ROW_NUMBER() OVER (
           PARTITION BY id
           ORDER BY depth ASC, pred ASC NULLS FIRST
         ) AS rn
  FROM reach
)
SELECT <visible projection of p>, t.depth AS __depth, t.pred AS __parent
FROM settled t JOIN <table> p ON p.<id> = t.id
WHERE t.rn = 1 AND <row-filters@p>
ORDER BY t.depth ASC, p.<id> ASC
```

Mechanics:

- **Predecessor.** The recursive term already binds `r.id` (the node being expanded); it
  is projected as `pred`. Seed rows carry `pred = NULL` — they are the roots. `CAST(NULL
  AS <id_sql_type>)` keeps the anchor and recursive column types union-compatible.
- **Settle + tie-break.** A node may appear at several `(depth, pred)` combinations
  (different paths, and — because `pred` now participates in the `UNION` dedup — several
  parents at the same depth). `ROW_NUMBER() OVER (PARTITION BY id ORDER BY depth ASC,
  pred ASC NULLS FIRST)` then `WHERE rn = 1` keeps exactly one row per node: **minimum
  depth** (the shortest path), and among equal-depth ties the **smallest predecessor
  identity**. Identity is the only stable, caller-independent ordering key available, so
  this is fully deterministic and independent of scan/join order — the property the union
  reachability CTE cannot otherwise guarantee. (`QUALIFY` is avoided — not universally
  supported by the DataFusion serving path; the `settled` CTE + `WHERE rn = 1` is the
  portable form.)
- **Roots included.** Reachability projects `depth >= 1` (membership excludes the seed
  unless a cycle re-reaches it). A *tree needs its roots*, so the tree projection keeps
  `depth = 0` rows; a seed that a cycle re-reaches still settles to `depth 0` (min), so it
  stays a parentless root, never a child of the cycle — correct for a rooted tree.
- **Termination / cost.** The `r.depth < N` bound still guarantees termination regardless
  of cycles. Carrying `pred` weakens the `UNION` frontier dedup from `(id, depth)` to
  `(id, depth, pred)`, so the recursion can enumerate more intermediate rows before the
  depth cap; the cap (max 10, default 5) remains the safeguard, and the `settled` step
  collapses the enumeration back to one row per node. A tighter visited-set prune stays a
  future optimization, as in reachability.
- **Multiple seeds → forest.** Each seed is a root; a non-seed reachable from several
  seeds settles to whichever gives min depth (then min parent). The result is a
  shortest-path **forest**; the response's root list names every root.

The compiler is a variant of `compile_graph_reach` (single self-link and path-cycle share
it): same `path: &[GraphStep]`, same seed/row-filter/param discipline and emission order,
plus the `pred` column, the `settled` window, and the depth/parent output columns. Param
order is unchanged (seed predicates → seed filters → recursive filters → projection
filters); the window carries no bound params.

## Response shape

**Selection: a response-shaping flag on the existing routes, not a new route.** The
traversal, self-link/path-cycle resolution, depth/`_ids`/filter parsing, and ACL are
identical to reachability — only the outer query and JSON differ. A `?tree=true` (or
`?view=tree`) param on `GET /objects/:type/graph/:link` and `GET /objects/:type/graph`
switches the response, so the tree does not duplicate the route/handler plumbing. Absent
the flag, the routes return the reachable set exactly as today (backward compatible). This
mirrors part-2/part-B's precedent of dispatching *within* the `?path=` handler by flag
rather than proliferating routes.

The tree response is a **distinct top-level shape** (not the flat `{objects:[…]}`, so a
client never confuses set vs tree):

```json
{
  "roots": [5],
  "nodes": [
    { "id": 5, "depth": 0, "parent": null, "object": { <typed props> } },
    { "id": 7, "depth": 1, "parent": 5,    "object": { <typed props> } },
    { "id": 9, "depth": 2, "parent": 7,    "object": { <typed props> } }
  ]
}
```

- `nodes` is ordered by `(depth, id)` — a stable, render-friendly order (roots first,
  BFS layers in sequence).
- `id` / `parent` are the node's and predecessor's **identity values**, rendered through
  the identity property's logical type (reusing `render_cell`, as `associations_to_json`
  renders ids). `parent: null` marks a root.
- `object` is the governed typed projection — the same per-property rendering
  `objects_to_json` produces (masked columns still masked, denied columns still absent).
- **Parent pointers, not materialized paths** — O(nodes) payload, and the client
  reconstructs any route by walking parents (or folds the whole forest). Materialized
  paths would be O(nodes · depth) and add nothing recoverable.

Implementation: a new handler result (e.g. `ObjectTree { roots, nodes }`, each node
carrying the object cells + `depth` + `parent`) built by splitting the trailing
`__depth`/`__parent`/id columns off each served row, and a `tree_to_json` renderer in
`render.rs` alongside `objects_to_json`. `read_graph_reach` gains a sibling
`read_graph_tree` (or a `tree: bool` on `GraphQuery` selecting the compiler + result
shape).

## ACL / governance

The tree inherits reachability's guarantee and adds one precondition.

- **No denied intermediate can be a parent.** Row-filters are AND'd into the seed, *every
  recursive expansion*, and the projection — so a denied/filtered row is never in `reach`
  at all, and therefore can never appear as another node's `pred`. The shortest path is
  computed over the **permitted subgraph only**: a node reachable *only* through a denied
  intermediate is simply not reachable (as in reachability), and every parent pointer
  emitted points at a row the caller may see. The path edges cannot leak a denied
  intermediate.
- **Identity must be visible for the tree** — stronger than reachability. Reachability
  needs identity only *declared* (an internal CTE key, never projected, so a denied/masked
  identity does not leak). The tree *projects* identity as `id` and `parent`, so a masked
  or denied identity **cannot** be served without leaking it. Precondition: if the type's
  identity column is denied or masked by policy, the tree read returns `Forbidden` (403,
  opaque — no column named), the same posture `association` takes (it too projects ids and
  requires identity visibility). If identity is undeclared → `NoIdentity` (400), as in
  reachability.
- Read-gate on `:type` (and every intermediate type in a `?path=` cycle) before existence
  is revealed; empty object projection → `Forbidden`. Unchanged from reachability.

## Error handling

Reuse the existing `graph_error` mapping — no new variants for slice 1:

- `NoIdentity` (undeclared identity) → 400.
- Masked/denied identity, or empty projection → `Forbidden` → 403 (opaque).
- `NotCyclicPath` (non-self link / non-cyclic path) → 400; `UnknownType`/`UnknownLink` →
  404; `BadFilter`/bad `depth` → 400; backend faults → opaque 500.
- `?tree=` parse: any value other than the accepted truthy/falsey token → 400.

## Testing

`rust_test` **integration** targets only (never inline `#[test]`); e2e via
`loom_fixture_test`. Reuse `//src/services/query-api:e2e-support` (`get`, `tref`, `land`,
`prop`, `grant_read`, `subject_with_role`, and the id extractors); extend it with a small
tree-node extractor (`id`/`depth`/`parent` per node) rather than re-parsing per file.

- **Compiler unit** (`tests/compile_graph_tree.rs`): the emitted SQL carries `pred` in
  the `reach` CTE (NULL at the anchor, `r.id` in the recursive term), the `settled` window
  `ROW_NUMBER() OVER (PARTITION BY id ORDER BY depth ASC, pred ASC …)` with `WHERE rn =
  1`, **includes `depth = 0` roots** (unlike reachability's `depth >= 1`), projects
  `__depth`/`__parent`, and binds seed/filter params in the same order as
  `compile_graph_reach` (regression: the reachable-set SQL is unchanged when the flag is
  off). Cover FK and join-table backings and a 2-step path-cycle.
- **Handler test** (`tests/graph_tree.rs`): masked/denied identity → `Forbidden`;
  undeclared identity → `NoIdentity`; happy path returns nodes with a parentless root and
  correct parent pointers on the stub graph.
- **Determinism e2e** (`tests/graph_tree_e2e.rs`, DataFusion serving): seed a graph with
  **two distinct shortest paths to one node** — e.g. `1→2, 1→3, 2→4, 3→4` (node 4 is
  depth-2 via parent 2 *or* 3). Assert `parent(4) == 2` (the smaller identity) and that
  the assertion holds across **repeated runs** — pinning the tie-break as scan/join-order
  independent, the property a set-returning query cannot guarantee.
- **Tree reconstruction e2e**: from a linear-plus-branch graph, walk each node's `parent`
  back to a root and assert every node's reconstructed depth equals its reported `depth`
  (the parent pointers form a valid rooted tree, no dangling parent).
- **Cycle e2e**: a `1→2→3→1` cycle terminates; each node settles to its min depth; a seed
  re-reached by the cycle stays a `depth 0` parentless root.
- **ACL e2e**: a Read row-filter blocking an intermediate node prunes everything reachable
  only through it, and **no surviving node reports the blocked node as its parent**
  (proving path edges don't leak a denied intermediate).
- **Forest e2e**: two seeds produce two roots; a node reachable from both settles to the
  min-depth (then min-parent) root deterministically.

## Non-goals

- **Weighted shortest paths** ([[fut-weighted-edges]]) — BFS gives unweighted (fewest-hop)
  paths only.
- **Tree over the union (`?links=`) or recursive-core+tail (part B) routes** — slice 1 is
  the self-link and path-cycle routes that share `read_graph_reach`.
- **Materialized full paths / k-shortest / all-shortest-paths** — parent pointers
  reconstruct the single shortest-path tree; enumerating alternatives is out.
- **Target-scoped queries** (a `?to=` shortest-path-to-one-node endpoint) — subsumed by
  walking parents client-side; no server surface added.

## Open questions

1. **Flag vs sub-route.** `?tree=true` on the existing routes (recommended, minimal
   plumbing) vs a dedicated `/graph/:link/tree` + `/graph/tree` route (more discoverable,
   more duplication). Leaning to the flag for slice 1.
2. **Row limit vs tree integrity.** Reachability applies a `LIMIT`. A `LIMIT` on a tree
   can drop a parent while keeping a child → a dangling `parent` pointer. Recommend
   **dropping the row limit for the tree** and relying on the depth cap (a depth-N tree is
   inherently bounded) so the response is always a complete, consistent forest — or, if a
   node cap is required, documenting that clients must tolerate absent parents. Needs a
   decision.
3. **Tie-break key.** Smallest predecessor *identity* is the only stable caller-independent
   key. Is min-identity the desired tie-break, or should a caller be able to bias it (e.g.
   a stable edge/insertion order)? Identity is the default; anything richer is deferred.
4. **DataFusion window support.** Confirm the engine's DataFusion version supports
   `ROW_NUMBER() OVER (PARTITION BY … ORDER BY …)` in a CTE (it should); the fallback is a
   correlated `MIN(depth)` + `MIN(pred)` self-join, which is portable but heavier. Verify
   before committing the window form.
5. **Depth field exposure.** The tree exposes per-node `depth`, which closes
   [[fut-min-depth-annotation]] for the tree shape — should reachability's set response
   *also* gain an optional `depth` annotation for parity, or is that left to the tree
   surface alone?
