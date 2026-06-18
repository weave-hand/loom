# `/graph` part-1: bounded recursive reachability — design (2026-06-18)

> Query-pillar slice. The first **graph** traversal surface — what the relational
> `/links` path structurally refuses. Every relational slice
> (`2026-06-15-query-multi-hop-traversal-design.md`,
> `2026-06-16-query-target-intermediate-filters-design.md`) drew a "relational vs graph"
> boundary and rejected cyclic / self-link / variable-length paths, pointing at a deferred
> `/graph` capability. This delivers its load-bearing primitive: bounded recursive
> reachability over a single self-link.

## Motivation

`/links` compiles a fixed number of `INNER JOIN`s — one per hop — so it cannot answer
"who is reachable from X within N hops?" over a self-referential link
(`Person --knows--> Person`). That is a transitive-closure question requiring recursion.
This slice adds a `/graph` surface that serves it as a depth-bounded `WITH RECURSIVE`
query (DuckDB-native), governed at every expansion and deduped by the type's first-class
identity — so it builds directly on the identity + object-set work.

## Scope

In scope — the part-1 primitive:

- `GET /objects/:type/graph/:link?depth=N` → the deduped set of `:type` objects reachable
  from the seed set via 1..N hops of `:link`, where `:link` is a **self-link**
  (`from == to == :type`).
- Governed (Read on the type; row-filters applied to the seed, every recursive
  expansion, and the final projection; denied/masked columns handled by the existing
  projection machinery).
- Seeds from the existing `?_ids=` and source-position filters; absent → all objects.
- Returns reachable objects (`objects_to_json`), reusing the existing render.

Out of scope (future `/graph` parts, recorded in `docs/FUTURE.md`):

- Multi-link / heterogeneous graph paths; graph-aware filter addressing across a cyclic
  path; min-depth annotation; shortest-path / `/tree`; weighted edges.

## Surface

`GET /objects/:type/graph/:link` with query params:

- `?depth=N` — max hop count. `1 ≤ N ≤ MAX_GRAPH_DEPTH` (cap **10**); absent → default
  **5**; `< 1` or `> cap` → `400`.
- `?_ids=v1,v2` — seed identities (reuses the shipped object-set input).
- source-position filters (bare `?col=op:val`) — narrow the seed set.

Example: `GET /objects/Person/graph/knows?depth=3&_ids=5` — `Person`s reachable from
person 5 within 3 `knows` hops.

## Constraints (resolved in the handler)

- **Self-link required.** The named link must resolve with `from == to == :type`. A
  non-self link → `QueryError::NotSelfLink` (`400`), message pointing at `/links` for
  fixed-length relational paths. (Only a link whose endpoints are the same type can be
  followed repeatedly.)
- **Declared identity required.** The type must declare an `identity` (the recursion's
  visited/dedup key) → `QueryError::NoIdentity` (`400`) if absent.
- **Identity visibility is NOT required.** The identity is an internal CTE key (used in
  joins / `IN` / dedup), never projected unless it is independently a visible column —
  so a denied/masked identity does not leak through a `/graph` read, and the read still
  works. (This is weaker than association, which *projects* the ids.)

## Compiler — `compile_graph_reach`

A new `sql.rs` entry point emitting a `WITH RECURSIVE` CTE, reusing the per-hop join
shapes (FK / join-table) and `filter_sql` / `caller_predicate_sql` from
`compile_chain_with`:

```sql
WITH RECURSIVE reach(id, depth) AS (
  SELECT s.<id>, 0 FROM <table> s WHERE <seed predicates + row-filters>
  UNION
  SELECT nxt.<id>, r.depth + 1
  FROM reach r
  JOIN <table> cur ON cur.<id> = r.id
  <link join to nxt>                         -- FK: JOIN <table> nxt ON cur.<from_col> = nxt.<to_col>
                                             -- JoinTable: JOIN <jt> j ON cur.<from_key> = j.<from_col>
                                             --            JOIN <table> nxt ON j.<to_col> = nxt.<to_key>
  WHERE r.depth < <N> AND <row-filters on nxt>
)
SELECT DISTINCT <visible projection of p>
FROM <table> p
WHERE p.<id> IN (SELECT id FROM reach WHERE depth >= 1)
  AND <row-filters on p>
<limit>
```

Signature (mirrors `compile_chain_pairs`'s shape):

```rust
pub fn compile_graph_reach(
    dialect: &dyn SqlDialect,
    table: &TableRef,
    identity: &str,
    backing: &LinkBacking,
    seed_predicates: &[CallerPredicate],   // source filters + the _ids In-predicate, at the seed
    row_filters: &[RowFilter],             // the type's ACL row-filters (seed, expansion, projection)
    allowed_cols: &[String],               // visible projection
    mask_cols: &[String],
    depth: u32,
    limit: u32,
) -> Result<(String, Vec<SqlValue>), CompileError>;
```

Notes:

- **Termination** is guaranteed by the `r.depth < N` bound regardless of cycles; the outer
  `SELECT DISTINCT` dedups the reachable node set. (`UNION`, not `UNION ALL`, also dedups
  identical `(id, depth)` rows within the CTE.) A tighter visited-set prune is a future
  optimization; the depth cap is the MVP safeguard.
- The same `row_filters` are rendered three times (seed `WHERE`, recursive `WHERE`,
  projection `WHERE`), each bound at the appropriate alias, so the parameter vector is
  built in emission order to keep positional `?` alignment — same discipline as
  `compile_chain_with`.
- Reachability is `depth >= 1` (a node appears iff reached by ≥1 hop; a seed reappears
  only if a real path of length ≥1 reaches it, e.g. via a cycle).

## Handler — `read_graph_reach`

```rust
pub struct GraphQuery {
    pub type_name: String,
    pub link: String,
    pub depth: u32,
    pub filters: Vec<(String, String)>,   // bare source filters (col, raw)
    pub ids: Vec<String>,                 // ?_ids= seeds
}

pub async fn read_graph_reach(
    q: &GraphQuery,
    subject: &Subject,
    deps: &QueryDeps<'_>,
) -> Result<ObjectRows, QueryError>;
```

Flow:

1. Read gate on `:type` (deny-by-default, before existence is revealed).
2. Resolve the type; load policy (`row_filters`, `denied`, `masked`).
3. Require `identity` (`NoIdentity` if absent).
4. Resolve `:link` from the type's outbound links; require `from == to == type`
   (`NotSelfLink` otherwise). Take its `backing`.
5. Build seed predicates: coerce each source filter via `coerce_predicate` (visibility-
   checked against the projection, like `read_object`) and append the `identity_in_predicate`
   for `?_ids=` (reusing the shipped helper). All bound at the seed.
6. Project allowed columns (minus denied), mask list; empty projection → `Forbidden`.
7. `compile_graph_reach(...)` → `fetch_rows` → `ObjectRows` (logical types from the type's
   properties, exactly as `read_object` builds them).

Depth validation (`1..=MAX_GRAPH_DEPTH`) happens at the HTTP edge (so the handler trusts a
valid `depth`); an out-of-range depth never reaches the compiler.

## HTTP

New route `GET /objects/:type_name/graph/:link_name`. The handler:

- parses `?depth=` (default 5; `<1` or `>10` → `400`), `?_ids=` (reusing the object-set
  parsing, empty → `400`), and the remaining params as source filters;
- calls `read_graph_reach`; renders `objects_to_json`;
- maps errors via a small match: `NotSelfLink`/`NoIdentity`/`BadFilter`/`BadChain` → `400`,
  `UnknownType`/`UnknownLink` → `404`, `Forbidden` → `403`, else opaque `500` (no internal
  detail leaked).

## Governance summary

`Read` on the type, then the type's row-filters AND'd into the seed, **every recursive
expansion**, and the final projection — so a caller reaches only nodes routed entirely
through rows the policy permits (the single-type analog of the relational N-ends
guarantee). Denied columns are projected out; masked columns masked. The `?_ids=`/source
filters only narrow the seed within already-permitted rows. Identity need only be
*declared*, not visible.

## Testing

- **Compiler unit** (`compile_graph_reach`): the emitted SQL is a `WITH RECURSIVE` that
  projects the visible columns of `p`, joins via the backing (FK and join-table cases),
  bounds on `depth`, restricts to `depth >= 1`, and binds seed/filter params in order.
- **e2e** (DuckDB, real HTTP router): seed a `Person --knows--> Person` self-link graph.
  - reachable-within-2 vs reachable-within-3 differ (depth bound works);
  - a **cycle** (A→B→A) terminates and the node set is deduped;
  - a row-filter ACL policy on `Person` prunes which nodes are reachable *through* (a
    blocked intermediate cuts off everything beyond it);
  - `?_ids=` seeds scope the start set;
  - a non-self link → `400` (`NotSelfLink`);
  - `?depth=0` and `?depth=99` → `400`.

## Task breakdown

1. **Compiler** — `compile_graph_reach` (recursive CTE; FK + join-table backings; the
   three-position row-filter rendering); compiler unit tests.
2. **Handler** — `GraphQuery`, `read_graph_reach`, `QueryError::NotSelfLink`; the
   self-link / identity / seed-predicate resolution.
3. **HTTP + e2e** — the `/objects/:type/graph/:link` route, depth/`_ids`/filter parsing,
   error mapping; the DuckDB reachability e2e.
4. **Docs** — roadmap delivered marker; `docs/FUTURE.md` (graph part-1 delivered; the
   remaining `/graph` parts).
