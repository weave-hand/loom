# `/graph` part-3: multi-edge union reachability — design (2026-06-19)

> Query-pillar slice, continuing the graph arc. Adds a second axis to `/graph`: where
> part-1 (`2026-06-18-graph-reachability-design.md`) repeats a single self-link and part-2
> (`2026-06-18-graph-path-cycle-design.md`) repeats a cyclic *path*, this slice repeats a
> **set** of self-links — each recursive step follows **any one** of N self-links. New
> `?links=` query param, distinct from part-2's `?path=`.

## Motivation

Part-1/2 repeat a fixed edge pattern. Many graph questions want "reachable via *any* of
these relationships": colleagues-or-friends, follows-or-mentors, parent-or-guardian. Each
recursive hop should be allowed to follow any one of a named set of self-links, mixing FK-
and join-table-backed links freely. This is a union over edge types at every step — a
different generalization from part-2's ordered path (a sequence), and the two compose later
(option B / future).

## Scope

In scope:

- `GET /objects/:type/graph?links=l1,…,lN&depth=D` — objects of `:type` reachable from the
  seed by repeatedly following **any one** of the named links, up to `D` times. Every named
  link must be a **self-link** on `:type` (`from == to == :type`).
- Governed: `Read` on the queried type and the queried type's row-filters at the seed, every
  recursive landing node, and the projection. Declared identity required (the dedup key).
- Mixed backings: the named set may freely mix FK- and join-table-backed self-links.

Out of scope (later `/graph` parts / future, recorded in `docs/FUTURE.md`):

- Recursive-core + relational-tail (`path=knows*,worksAt` — option B), and composing the
  union axis with the path axis (a set of *paths* per step).
- Inverse links in the set, min-depth annotation, shortest-path / `/tree`, weighted edges.

## Governance (simpler than part-2)

Because every named link is a self-link on the queried type, the recursion never lands on a
foreign type — so there are **no intermediate types**. Governance reduces to a single type's
policy:

- One `Read` gate: the queried type (deny-by-default, before existence is revealed).
- The queried type's row-filters, applied at the seed `s`, at **every** arm's landing node
  `nxt` (so each edge type expands only through permitted rows), and at the projection `p`.
- Declared identity required (the recursion's dedup key; visibility not required, as in
  part-1/2).

No per-link Read gates and no per-link row-filter loads — every link resolves to the queried
type, already gated.

## Compiler — new sibling `compile_graph_reach_union`

Part-2's `compile_graph_reach` (path) is left **untouched**. A sibling compiler emits the
**N-arm union** recursive term: the recursive part of the CTE is a `UNION` of one SELECT per
named link, each a single self-hop `cur → nxt`.

```rust
/// Compile a depth-bounded recursive reachability query over a UNION of self-links: the
/// deduped set of `table` rows reachable from the seed set by repeatedly following ANY ONE
/// of `backings` (each a self-link on `table`) up to `depth` times. The recursive term is a
/// UNION of one arm per backing; every arm lands on `nxt` (= `table`) and is governed by the
/// start `row_filters`. Termination by the inlined `depth` bound; `DISTINCT` dedups. Every
/// caller value is a bound param.
#[allow(clippy::too_many_arguments)]
pub fn compile_graph_reach_union(
    dialect: &dyn SqlDialect,
    table: &TableRef,             // queried type's table (= every link's from and to)
    identity: &str,
    backings: &[LinkBacking],     // >= 1 self-link backings; each arm lands back on `table`
    seed_predicates: &[CallerPredicate],
    row_filters: &[RowFilter],    // queried type's filters: seed s + each arm's nxt + projection p
    allowed_cols: &[String],
    mask_cols: &[String],
    depth: u32,
    limit: u32,
) -> Result<(String, Vec<SqlValue>), CompileError>;
```

Emitted SQL (two links shown — a FK `knows` and a join-table `colleagues`):

```sql
WITH RECURSIVE reach(id, depth) AS (
  SELECT s."id", 0 FROM main.person s WHERE <seed preds@s + row_filters@s>
  UNION
  SELECT nxt."id", r.depth + 1 FROM reach r JOIN main.person cur ON cur."id" = r.id
    JOIN main.person nxt ON cur."knows_id" = nxt."id"                       -- arm 0 (FK)
    WHERE r.depth < 3 AND <row_filters@nxt>
  UNION
  SELECT nxt."id", r.depth + 1 FROM reach r JOIN main.person cur ON cur."id" = r.id
    JOIN main.colleagues j1 ON cur."id" = j1."a" JOIN main.person nxt ON j1."b" = nxt."id"  -- arm 1 (join table)
    WHERE r.depth < 3 AND <row_filters@nxt>
)
SELECT DISTINCT <proj@p> FROM main.person p
WHERE p."id" IN (SELECT id FROM reach WHERE depth >= 1) AND <row_filters@p>
LIMIT 1000
```

Notes:

- **Arm join shapes** are the single self-hop `cur → nxt` reused verbatim from the part-2
  per-step construction. FK: `cur.<from_column> = nxt.<to_column>`. Join-table: the two-join
  form via a per-arm alias `j{i}` (arm index `i`, so multiple join-table links never collide).
- **Self-hop join helper.** Extract the FK/join-table single-hop join construction into one
  private helper (`link_join`) and rewire `compile_graph_reach` to use it, then reuse it in
  `compile_graph_reach_union`. The extraction must be byte-identical (the part-1/2 single- and
  multi-step regression tests pin the emitted SQL); the helper takes the join-table alias as a
  parameter so each compiler supplies its own (`j` / `j{i+1}` for the path; `j{i}` for the
  union).
- **Param order** is SQL-emission order: seed predicates, then `row_filters@s`, then for each
  arm in `backings` order `row_filters@nxt`, then `row_filters@p`. `depth`/`limit` inlined;
  every caller value bound; identifiers double-quoted. (Row-filters are rendered N+2 times —
  once at the seed, once per arm at `nxt`, once at the projection.)
- `validate_row_filter` runs on every `row_filters` entry before any `filter_sql`.
- `backings` is non-empty (the handler enforces; an empty set never reaches the compiler).

## Handler — new `read_graph_reach_union`

A new query struct and handler, parallel to `read_graph_reach`:

```rust
/// A bounded recursive reachability read over a UNION of self-links. `filters`/`ids` scope
/// the SEED set; the recursion follows ANY ONE of `links` (each a self-link on `type_name`)
/// up to `depth` times.
pub struct GraphUnionQuery {
    pub type_name: String,
    pub links: Vec<String>,
    pub depth: u32,
    pub filters: Vec<(String, String)>,
    pub ids: Vec<String>,
}

pub async fn read_graph_reach_union(
    q: &GraphUnionQuery,
    subject: &Subject,
    deps: &QueryDeps<'_>,
) -> Result<ObjectRows, QueryError>;
```

Flow:

1. Read-gate `:type`; resolve the type; load its policy (`row_filters`, `denied`, `masked`);
   require declared `identity` (`NoIdentity` otherwise).
2. **Resolve the link set.** `links` empty → `NotCyclicPath` (defensive; the HTTP edge already
   rejects empty). Resolve the queried type's outbound links once. For each name in `q.links`
   (deduped, preserving first-seen order): missing → `UnknownLink` (404); `link.to != :type`
   (not a self-link) → `NotCyclicPath(name)` (400). Collect the backing. Result is a
   `Vec<LinkBacking>`. No per-link Read gate or row-filter load (every link lands on the
   already-gated queried type).
3. Projection (allowed minus denied; mask list; empty → `Forbidden`) and seed predicates
   (source filters coerced + visibility-checked, plus `?_ids=` via `identity_in_predicate`)
   — identical to `read_graph_reach`.
4. `compile_graph_reach_union(... backings ...)` → `fetch_rows` → `ObjectRows` (queried type's
   logical types), rendered by the existing `objects_to_json`.

Error reuse: a non-self link reuses `QueryError::NotCyclicPath` (a self-link is the 1-cycle;
a link whose `to` is not the queried type does not return to it). No new error variant, so the
existing `NotCyclicPath → 400` mapping covers it.

## HTTP

The `GET /objects/:type_name/graph` route (today `get_graph_path`, `?path=`) gains a `?links=`
branch:

- Parse `?links=` (comma-split, drop empties) alongside `?path=`, sharing the existing
  `depth`/`_ids`/filter loop.
- Dispatch:
  - both `?path=` and `?links=` non-empty → `400` ("specify either path or links, not both").
  - `?links=` non-empty → `read_graph_reach_union` via a new `graph_union_respond` tail.
  - `?path=` non-empty → existing path-cycle (`read_graph_reach`), unchanged.
  - neither → `400` (message updated to mention both params).
- `?depth=` (1..=`MAX_GRAPH_DEPTH`, default `DEFAULT_GRAPH_DEPTH`) and `?_ids=` parse exactly
  as today.

Refactor: extract the result/error → response mapping shared by both graph tails into a
`graph_error(e)` helper (mirroring `chain_error`), so `graph_respond` (path) and
`graph_union_respond` (union) both reduce to "build query → call handler → `Ok` ⇒ JSON /
`Err` ⇒ `graph_error`". The error arms are unchanged (`UnknownType`/`UnknownLink` → 404,
`NotCyclicPath`/`NoIdentity`/`BadFilter` → 400, `Forbidden` → 403, opaque 500).

The single-self-link route `GET /objects/:type_name/graph/:link_name` (`get_graph`) is
unchanged — it remains the part-1 path-cycle of length 1.

## Testing

- **Compiler unit** (`compile_graph_reach_union`): two backings (FK + join-table) emit a
  two-arm `UNION` recursive term with the row-filter rendered at `s`, each arm's `nxt`, and
  `p`; the FK arm joins `cur.<fk> = nxt.<id>` and the join-table arm uses `j1`; the depth
  bound is inlined; param order is pinned (seed, `s`, arm0 `nxt`, arm1 `nxt`, `p`). A
  single-backing call emits a one-arm union. Identity dedup via `DISTINCT` + `depth >= 1`.
- **Handler test** (`read_graph_reach_union`, stub serving): a non-self link → `NotCyclicPath`;
  an unknown link → `UnknownLink`; an empty link set → `NotCyclicPath`; the happy path returns
  the stub rows; duplicate link names collapse to one arm.
- **e2e** (DuckDB, real router): a `Person` with a `knows` (FK self-link) and a `colleagues`
  (join-table self-link).
  - `?links=knows,colleagues&depth=D` returns the union-reachable Persons; `?links=knows`
    alone returns a subset (people reachable only via `colleagues` are absent).
  - a cycle terminates and the node set is deduped; distinct depths differ.
  - a `Person` Read row-filter (e.g. exclude one person) prunes the reachable set — proving
    the per-arm `nxt` governance inside the recursion.
  - `?path=…&links=…` together → `400`; an empty `?links=` → `400`.

## Task breakdown

1. **Compiler** — extract the `link_join` self-hop helper and rewire `compile_graph_reach` to
   it (byte-identical, guarded by existing tests); add `compile_graph_reach_union` (N-arm union
   recursive term, per-arm `nxt` governance); compiler unit tests (two-arm + single-arm).
2. **Handler** — `GraphUnionQuery` + `read_graph_reach_union` (self-link resolution + dedup,
   `NotCyclicPath` for non-self links, single-type governance); handler tests.
3. **HTTP + e2e** — `?links=` branch on the `/graph` route with path/links dispatch and the
   ambiguity 400; extract `graph_error`; the `knows`/`colleagues` DuckDB e2e.
4. **Docs** — roadmap + `docs/FUTURE.md` (part-3 delivered; remaining `/graph` parts).

## Addendum (2026-06-19, during implementation): edge-relation formulation

The "N-arm `UNION` recursive term" sketched above — one recursive `SELECT … FROM reach …` per
link — is **invalid on DuckDB**: a recursive CTE's recursive term may reference the CTE name only
ONCE, and multiple self-referencing arms raise `Binder Error: Circular reference to CTE`. The
implemented compiler preserves identical semantics and governance with a single recursive
self-reference: each backing contributes one **non-recursive** arm emitting `(from_id, to_id)`
pairs (via the same `link_join` shapes), the arms are combined with `UNION ALL` into an edge
relation, and the one recursive step joins `reach` to that relation, then to the landing node
`nxt`:

```sql
WITH RECURSIVE reach(id, depth) AS (
  SELECT s."id", 0 FROM person s WHERE <seed preds@s + row_filters@s>
  UNION
  SELECT e.to_id, r.depth + 1
  FROM reach r
  JOIN (
    SELECT cur."id" AS from_id, nxt."id" AS to_id FROM person cur JOIN person nxt ON cur."knows_id" = nxt."id"
    UNION ALL
    SELECT cur."id" AS from_id, nxt."id" AS to_id FROM person cur JOIN colleagues j1 ON cur."id" = j1."a" JOIN person nxt ON j1."b" = nxt."id"
  ) e ON r.id = e.from_id
  JOIN person nxt ON e.to_id = nxt."id"
  WHERE r.depth < 3 AND <row_filters@nxt>
)
SELECT DISTINCT <proj@p> FROM person p
WHERE p."id" IN (SELECT id FROM reach WHERE depth >= 1) AND <row_filters@p>
LIMIT 1000
```

Governance is unchanged (row-filters at the seed `s`, the landing node `nxt`, and the projection
`p`); the only observable difference is that the landing-node filter is rendered **once** (shared
across all arms) rather than once per arm, so param count drops from `seed + N·|row_filters| +
projection` to `seed + |row_filters| + projection`. The compiler unit tests assert this final
shape (`UNION ALL` arm count, the bare CTE-level `UNION`, both join shapes, the reduced param
count); the DuckDB-backed `graph-union-e2e` proves it executes and returns the correct reachable
sets.
