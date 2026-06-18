# `/graph` part-2: repeated path-cycle — design (2026-06-18)

> Query-pillar slice, continuing the graph arc. Generalizes `/graph` part-1
> (`2026-06-18-graph-reachability-design.md`) from a single repeated **self-link** to a
> repeated **path that forms a cycle**: follow `l1,…,lK` (which must return to the queried
> type) up to N times. Part-1's single self-link becomes the degenerate 1-element path.

## Motivation

Part-1 answers "who is reachable via repeated `knows`?" — a single self-link. Many graph
questions need a multi-link pattern repeated transitively: "people reachable via
shared-team membership" is `Person --memberOf--> Team --hasMember--> Person`, applied
repeatedly. The pattern is a **cycle** on `Person` (it starts and ends at the queried
type), so it can be repeated. This slice generalizes the recursive reachability primitive
to any such cyclic path, reusing the relational chain's per-hop join shapes inside the
recursion.

## Scope

In scope:

- `GET /objects/:type/graph?path=l1,…,lK&depth=N` — objects of `:type` reachable from the
  seed by repeating the path pattern `l1..lK` up to N times, where the path forms a cycle
  (`from == :type` and, after following all K links forward, the type is again `:type`).
- Governed: `Read` on the queried type AND every intermediate type in the cycle; the
  start type's row-filters at the seed/projection; each intermediate type's row-filters in
  the recursive join. Declared identity required (the dedup key), as in part-1.
- Part-1's `GET /objects/:type/graph/:link` route is retained and **forwards a 1-element
  path** into the same machinery (no duplicate code path).

Out of scope (later `/graph` parts, recorded in `docs/FUTURE.md`):

- **Inverse links inside the path** (each path link is followed forward in MVP; a cycle is
  formed by forward links that return to the start type, e.g. `memberOf` + `hasMember`).
- Multi-edge union reachability (`?links=` over a set of self-links — option C).
- Recursive-core + relational-tail (`path=knows*,worksAt` — option B).
- Min-depth annotation, shortest-path / `/tree`, weighted edges.

## Cyclic validation

Resolve `l1..lK` forward from `:type` (each via the type's outbound `links`, like the
relational chain resolver). After following all K links, the current type **must equal**
`:type`. Otherwise → `QueryError::NotCyclicPath(path)` → `400`, with a message pointing at
relational `/links` for non-cyclic fixed-length traversal.

**Decision — unify `NotSelfLink` into `NotCyclicPath`.** Part-1's single-link self-link
check (`from == to == type`) is exactly the 1-element case of cyclic validation. The
`QueryError::NotSelfLink` variant is **replaced** by `NotCyclicPath` (clearer for K>1 and
DRY); part-1's `/graph/:link` route resolves `[link]` as a 1-element path, so a non-self
link yields `NotCyclicPath` (still `400`). Part-1's e2e assertion updates from
`NotSelfLink` to `NotCyclicPath` (status unchanged).

Other rejections: an unknown link in the path → `UnknownLink` (404); a depth out of
`1..=MAX_GRAPH_DEPTH` (cap 10, default 5) → 400 (HTTP edge, unchanged from part-1).

## Compiler — generalize `compile_graph_reach`

Replace the single `backing: &LinkBacking` parameter with an ordered **path** of steps.
Each step carries the link's backing, the table of the type it lands on, and that type's
ACL row-filters:

```rust
pub struct GraphStep {
    pub backing: LinkBacking,
    pub next_table: TableRef,        // the table this hop lands on (the final hop lands on the start table)
    pub next_filters: Vec<RowFilter>, // the landed type's ACL row-filters (intermediate governance)
}

pub fn compile_graph_reach(
    dialect: &dyn SqlDialect,
    table: &TableRef,                 // the start (= queried) type's table
    identity: &str,
    path: &[GraphStep],               // >= 1 step; the last lands back on `table`
    seed_predicates: &[CallerPredicate],
    row_filters: &[RowFilter],        // the start type's row-filters (seed + projection + final nxt)
    allowed_cols: &[String],
    mask_cols: &[String],
    depth: u32,
    limit: u32,
) -> Result<(String, Vec<SqlValue>), CompileError>;
```

The recursive CTE step joins `cur` through the whole path to `nxt` (both the start type),
reusing the existing per-hop FK / join-table join shapes (one fresh alias `g{i}` per
intermediate landing, `nxt` for the final):

```sql
WITH RECURSIVE reach(id, depth) AS (
  SELECT s.<id>, 0 FROM <table> s WHERE <seed predicates + start row-filters at s>
  UNION
  SELECT nxt.<id>, r.depth + 1
  FROM reach r JOIN <table> cur ON cur.<id> = r.id
    JOIN <step1.next_table> g1 ON <step1 backing: cur -> g1>
    JOIN <step2.next_table> g2 ON <step2 backing: g1 -> g2>
    ...
    JOIN <table> nxt ON <stepK backing: g_{K-1} -> nxt>
  WHERE r.depth < <depth>
    AND <step_i.next_filters at g_i for each intermediate>   -- intermediate governance
    AND <start row-filters at nxt>                            -- final landed node governed
)
SELECT DISTINCT <projection of p> FROM <table> p
WHERE p.<id> IN (SELECT id FROM reach WHERE depth >= 1) AND <start row-filters at p>
<limit>
```

Notes:

- **Join shapes** per step reuse the part-1 / chain construction. FK:
  `from_alias.<from_column> = to_alias.<to_column>`. Join-table: the two-join form via a
  per-step `j{i}` alias.
- **Param order** is SQL-emission order: seed predicates, seed start-filters, then for the
  recursive term each step's `next_filters` (at `g_i` / `nxt`) in path order, then the
  start filters at `nxt`, then projection start-filters at `p`. Same discipline as part-1
  (every value bound; `depth`/`limit` inlined).
- `validate_row_filter` runs on every filter (start + each step's) before any `filter_sql`.
- The last step's `next_table` is the start table and its `next_filters` are the start
  type's row-filters (the cycle closes on the queried type); the compiler applies the
  start filters at `nxt` regardless, so a caller may pass the final step's `next_filters`
  empty to avoid double-rendering — **the handler passes intermediates only** (steps
  `1..K-1` carry their type's filters; step K carries an empty `next_filters`, since `nxt`
  is governed by `row_filters`). This keeps each filter rendered once per position.
- A 1-element path (part-1) has no intermediates: the single step lands directly on `nxt`
  with empty `next_filters`, identical to part-1's emitted SQL.

## Handler — generalize `read_graph_reach`

`GraphQuery` carries a `path: Vec<String>` (was a single `link: String`):

```rust
pub struct GraphQuery {
    pub type_name: String,
    pub path: Vec<String>,
    pub depth: u32,
    pub filters: Vec<(String, String)>,
    pub ids: Vec<String>,
}
```

Flow (generalizing part-1):

1. Read gate on `:type`; resolve the type; load its policy (`row_filters`, `denied`,
   `masked`); require declared `identity`.
2. **Resolve the path-cycle.** Walk `l1..lK`: at each step resolve the current type's
   outbound link by name (`UnknownLink` if missing), take its `to` type and `backing`,
   `Read`-gate the landed type (`Forbidden` if denied), and load its row-filters. After
   the last step the current type must be `:type` (`NotCyclicPath` otherwise). Build the
   `Vec<GraphStep>`: steps `1..K-1` carry the landed (intermediate) type's row-filters;
   the final step carries empty `next_filters` (the start type's filters are passed
   separately and rendered at `nxt`).
3. Projection (allowed minus denied; mask list; empty → `Forbidden`) and seed predicates
   (source filters coerced + visibility-checked, plus `?_ids=` via `identity_in_predicate`)
   — unchanged from part-1.
4. `compile_graph_reach(... path ...)` → `fetch_rows` → `ObjectRows` (start type's logical
   types), rendered by the existing `objects_to_json`.

The path's empty case (`path.is_empty()`) → `400` (`NotCyclicPath` or a length check); the
depth-cap is enforced at the HTTP edge.

## HTTP

- New route `GET /objects/:type_name/graph` with `?path=l1,l2` (comma-split), `?depth=`,
  `?_ids=`, and source filters — parsed exactly like part-1's `get_graph`, plus `path`.
  An empty/absent `?path=` → `400`.
- Part-1's `GET /objects/:type_name/graph/:link_name` route is retained; its handler builds
  `path: vec![link_name]` and calls the same `read_graph_reach`.
- Error mapping adds `NotCyclicPath → 400` (replacing `NotSelfLink`); the rest unchanged.

## Governance summary

`Read` on the queried type and **every intermediate type** in the cycle; the start type's
row-filters at the seed, the final landed node (`nxt`), and the projection; each
intermediate type's row-filters in the recursive join. A recursive step therefore expands
only through intermediate rows the policy permits — the relational N-ends guarantee, now
applied inside each recursive application of the pattern. Identity need only be declared.

## Testing

- **Compiler unit** (`compile_graph_reach`): a 2-step path (FK + join-table) emits the
  multi-join recursive term with each intermediate's row-filter at its alias and the start
  filter at `nxt`; a 1-step path matches part-1's single-link SQL (regression). Param
  ordering pinned.
- **Handler test**: a non-cyclic path → `NotCyclicPath`; an unknown path link →
  `UnknownLink`; a forbidden intermediate type → `Forbidden`; happy path returns the stub
  rows.
- **e2e** (DuckDB, real router): the `Person --memberOf--> Team --hasMember--> Person`
  shared-membership graph.
  - `?path=memberOf,hasMember&depth=K` from a seed returns the transitively-connected
    Persons; distinct depths differ.
  - a cycle terminates and the node set is deduped.
  - a Read row-filter on the **intermediate** `Team` (e.g. exclude one team) prunes the
    Persons reachable only through that team — proving intermediate governance inside the
    recursion.
  - a non-cyclic `?path=memberOf,worksAt` (ends at Company) → `400`.
  - part-1's single-self-link route still works (`/graph/:link`), and a non-self link there
    now → `400` (`NotCyclicPath`).

## Task breakdown

1. **Compiler** — generalize `compile_graph_reach` to a `&[GraphStep]` path (multi-link
   recursive join, per-intermediate row-filters); update the part-1 single-link call site;
   compiler unit tests (multi-step + 1-step regression).
2. **Handler** — `GraphQuery.path`, path-cycle resolution + `NotCyclicPath` (replacing
   `NotSelfLink`), intermediate `Read`+row-filter governance; handler tests.
3. **HTTP + e2e** — `GET /objects/:type/graph?path=` route; `/graph/:link` forwards
   `[link]`; `NotCyclicPath`→400 mapping; the shared-membership DuckDB e2e (incl. the
   part-1 single-link assertion update).
4. **Docs** — roadmap + `docs/FUTURE.md` (part-2 delivered; remaining `/graph` parts).
