# `/graph` part B — recursive-core + relational-tail — Design

> The fourth `/graph` slice (after part-1 single self-link reachability, part-2
> repeated path-cycle, part-3 multi-edge union). Closes the recursion-then-tail
> shape: a depth-bounded recursive core followed by a fixed acyclic relational
> tail to a (possibly different) type.

## Goal

Serve `GET /objects/:type/graph?path=knows*,worksAt` — reach a set of objects
through 1..D hops of a single self-link (the recursive **core**), then continue
from that reachable set through a fixed, forward, acyclic chain of ordinary links
(the relational **tail**), projecting the tail's final type. "The companies of
everyone reachable from X through `knows`."

This is the one `/graph` shape that combines recursion with cross-type traversal:
the core reuses part-1's recursive CTE; the tail reuses the existing multi-hop
chain compiler; the two are glued by a reachable-set membership predicate.

## Surface

`GET /objects/:type/graph?path=<core>*,<tail…>&depth=N`

- The `?path=` route already exists (part-2 cyclic path). A `*` suffix on a path
  segment switches that request into part B.
- **Exactly one `*`, and it must be on the first segment** — the recursive core
  is a path *prefix*. The starred link must be a **self-link** on `:type` (it
  lands back on `:type`).
- Everything after the starred segment is the **relational tail**: one or more
  forward links, chained, landing on a final type `F` (possibly `≠ :type`).
- `depth` (1–10, default 5) bounds the recursive core only; the tail is fixed
  length. `_ids=id1,id2` scopes the seed set, exactly as part-1/2/3.
- Leftover `?param=value` query params are **seed filters on `:type`** (the core
  source), exactly as part-1/2. The tail target gets no caller filters this slice
  (its ACL row-filters still apply). Positional/tail-target filter addressing is
  the already-deferred "graph-aware filter addressing" follow-on.

Routing within the `?path=` handler:

- `?path=` with **no** `*` → part-2 (`read_graph_reach`, cyclic path) — unchanged.
- `?path=` with a `*` → part B (`read_graph_reach_with_tail`).
- `?links=` → part-3 (union) — unchanged; `?path=`/`?links=` stay mutually
  exclusive (both present → 400).

## Approach (chosen: A)

The design axis is how the recursive core and relational tail combine in SQL.

- **A — one SQL statement: recursive CTE + chain-tail, glued by reach
  membership (chosen).** Emit part-1's `reach(id, depth)` CTE over the single
  self-link, then a `compile_chain_with`-style INNER-JOIN tail whose source
  position is `:type` constrained by
  `t_0."id" IN (SELECT id FROM reach WHERE depth >= 1)`. One round-trip, one
  `fetch_rows`, governance threaded per layer, reuses both existing building
  blocks. Minimal new code.
- **B — two statements: run reach, collect ids, then run the tail with
  `id IN (literals)`.** Two round-trips, unbounded literal lists, abandons the
  single-SQL compile/governance model every other read uses. Rejected.
- **C — fold the tail into `compile_graph_reach` itself (one mega-compiler).**
  Bloats part-1/2's carefully-ordered cyclic-path compiler with tail concerns and
  makes its param ordering harder to hold in context. Rejected in favour of a
  **separate** compiler that *shares* a small extracted CTE-builder helper (DRY
  without entanglement).

## SQL shape

```sql
WITH RECURSIVE reach(id, depth) AS (
  SELECT s."id", 0 FROM "sch"."T" s WHERE {seed preds + row_filters@T}
  UNION
  SELECT nxt."id", r.depth + 1 FROM reach r
    JOIN "sch"."T" nxt ON {core self-link join}      -- FK or join-table
    WHERE r.depth < D AND {row_filters@T}
)
SELECT DISTINCT {F cols, masked} FROM "sch"."T" t_0
  JOIN ... tail hops ... t_k                          -- chain_from_where
  WHERE t_0."id" IN (SELECT id FROM reach WHERE depth >= 1)
    AND {row_filters@C1 … @F}
```

- The CTE is byte-identical to part-1's single-self-link recursive CTE (a 1-step
  self-cycle): seed anchor, `UNION` (distinct, cycle-safe), `r.depth < D`
  inlined termination, self-link join via the shared `link_join` helper (FK or
  join-table).
- The tail's source position (`t_0`) is `:type` restricted to the reachable set
  (`depth >= 1`, excluding the seed unless a cycle re-reaches it). The tail then
  INNER-JOINs forward through each hop to `t_k = F`.
- `SELECT DISTINCT` over `F`'s projected columns dedups the result, so `F` needs
  no declared identity — only the core type does.

## Governance

Deny-by-default, layered, Read-gate before existence is revealed (matches
part-1/2/3 and the chain reads):

- **Core type `:type` (`T`)**: Read-gated first. Row-filters applied at the seed
  anchor *and* every recursive hop, inside the CTE. **Requires a declared
  identity** (`NoIdentity` otherwise) — it is the `reach` dedup key and the join
  key from the tail back to the reachable set.
- **Each tail-landed type, including `F`**: Read-gated; its row-filters applied
  at its alias. (Same per-position governance the existing chain reads use.)
- **`F`**: column projection — allowed/mask columns, deny-wins; masked columns
  rendered as the `'***'` marker.
- The tail's source position (`T`/`t_0`) carries **no additional row-filters**:
  `T`'s governance already lives in the CTE, and the reachable set it produces is
  exactly the `T`-ids that passed those filters. Re-applying would only duplicate
  bound params for no semantic effect.
- Caller `?param` filters are coerced/visibility-checked against `T`'s columns
  only (seed filters).

## New compiler

`sql.rs`:

```
fn recursive_reach_cte(
    dialect, table: &TableRef, identity: &str,
    backing: &LinkBacking, seed_predicates: &[CallerPredicate],
    row_filters: &[RowFilter], depth: u32,
) -> Result<(String /* "WITH RECURSIVE reach(id,depth) AS (…)" */,
             Vec<SqlValue>), CompileError>
```

A new, focused helper that builds the single-self-link recursive CTE (the
degenerate 1-step case of part-1's path-cycle CTE). It is **not** a refactor of
`compile_graph_reach` — that compiler builds the CTE for an arbitrary multi-link
path, and perturbing the tested part-1/2 compiler for marginal DRY is the wrong
trade. The genuinely shared surface is the self-link JOIN, which already goes
through the existing `link_join` helper; what `recursive_reach_cte` adds is the
short, single-self-link CTE skeleton (seed anchor, distinct `UNION`, inlined
`r.depth < D` termination). Param order: seed predicates → seed `row_filters@T`
→ recursive `row_filters@T`.

```
pub fn compile_graph_reach_tail(
    dialect: &dyn SqlDialect,
    table: &TableRef,            // core source type T
    identity: &str,              // T's identity / reach key
    core_backing: &LinkBacking,  // the *-suffixed self-link
    seed_predicates: &[CallerPredicate],
    core_row_filters: &[RowFilter],   // T's row-filters (seed + recursive)
    tail_types: &[ChainType],    // positions 0..k: t_0 = T (empty filters),
                                 //   t_1..t_k = landed types incl. final F
    tail_hops: &[LinkBacking],   // tail_types.len() == tail_hops.len() + 1
    allowed_cols: &[String],     // projection of final tail type F
    mask_cols: &[String],
    depth: u32,
    limit: u32,
) -> Result<(String, Vec<SqlValue>), CompileError>
```

Builds the CTE via `recursive_reach_cte`, the tail FROM/JOIN/WHERE via the
existing `chain_from_where`, and emits the membership-glued `SELECT DISTINCT`.
`tail_types[0]` is `T` with empty row-filters (governance is in the CTE); its
alias `t_0` is constrained by `t_0."identity" IN (SELECT id FROM reach WHERE
depth >= 1)`. Param order: seed predicates → seed `row_filters@T` → recursive
`row_filters@T` → tail per-position params (predicates then filters, from
`chain_from_where`). DuckDB dialect wrapper `compile_graph_reach_tail` mirrors the
existing wrapper pattern.

## New handler

`handler.rs`:

```
pub struct GraphTailQuery {
    pub type_name: String,
    pub core_link: String,        // the *-stripped first segment
    pub tail_links: Vec<String>,  // remaining segments, forward
    pub depth: u32,
    pub filters: Vec<(String, String)>,
    pub ids: Vec<String>,
}

pub async fn read_graph_reach_with_tail(
    q: &GraphTailQuery, subject: &Subject, deps: &QueryDeps<'_>,
) -> Result<ObjectRows, QueryError>
```

Flow:

1. Read-gate `:type` (deny-by-default before existence).
2. Resolve `:type` → `ObjectType`; require declared identity (`NoIdentity`).
3. Load `:type` policy → `row_filters@T`.
4. Resolve `core_link` → `LinkBacking`; assert it is a self-link (`link.to ==
   :type`), else `BadGraphPath` ("recursive core `<link>*` must land back on
   `:type`").
5. Require non-empty `tail_links`, else `BadGraphPath` ("recursive core requires a
   relational tail; use `/graph/:link` for bare reachability").
6. Resolve the forward tail: for each `tail_links[i]`, resolve forward,
   Read-gate the landed type, load its row-filters; assemble
   `tail_types = [ChainType{T, filters:[], preds:[]}, landed₁, …, F]` and
   `tail_hops`. Load `F`'s policy → projection (allowed/mask columns).
   `UnknownLink` on any unresolved name.
7. Build the seed set: coerce `?param` filters against `T`'s columns
   (`BadFilter` on bad value/column), and an identity `In` predicate from
   `q.ids` when present (object-set scoping).
8. `compile_graph_reach_tail` → `fetch_rows` → `ObjectRows`.

Tail resolution reuses the existing chain-resolution logic (Read gates +
per-position row-filters); the tail is forward-only this slice, so the inverse
branch of the general chain resolver is not exercised.

## Error handling

Add one variant to `QueryError`:

```
BadGraphPath(String)   // → HTTP 400
```

Used for the part-B structural/semantic path faults: `*` not on the first
segment, more than one `*`, empty tail, and core-link-not-a-self-link. Reuse
existing variants otherwise: `UnknownType`/`UnknownLink` → 404, `NoIdentity` →
400, `BadFilter` → 400, `Forbidden` → 403, backend faults → 500 opaque. The
`graph_error` mapping in `http.rs` gains the `BadGraphPath` → 400 arm.

## HTTP wiring

`http.rs` `get_graph_path`:

- Parse `path` (comma-split). If any segment carries a trailing `*`:
  - Structural validation here: exactly one `*`, on the first segment, else 400
    `BadGraphPath`. Strip the `*`; first segment → `core_link`, rest →
    `tail_links`.
  - Build `GraphTailQuery` (with `depth`, `_ids`, leftover params as `filters`)
    and dispatch to `read_graph_reach_with_tail`, mapping errors via
    `graph_error`.
- No `*` and non-empty `path` → part-2 (unchanged). `links` present → part-3
  (unchanged). `path` + `links` both present, or both empty → 400 (unchanged).

## Testing

Three targets, mirroring the part-3 layout, all `rust_test` (no inline tests);
the e2e via `loom_fixture_test` (DuckDB):

- **`compile_graph_reach_tail` unit** (`tests/compile_graph_reach_tail.rs`): FK
  tail, join-table tail, multi-hop tail; assert the `reach` membership predicate
  is present, `SELECT DISTINCT` over `F`'s columns, the `r.depth < D` bound, the
  core self-link join shape, and exact param order/count.
- **`read_graph_reach_with_tail` handler** (`tests/graph_reach_tail.rs`):
  core-not-self-link → `BadGraphPath`; unknown core/tail link → `UnknownLink`;
  empty tail → `BadGraphPath`; no-identity `:type` → `NoIdentity`; Read-denied
  tail type → `Forbidden`; happy path; dedup across distinct reach paths to the
  same `F` row.
- **DuckDB router e2e** (`tests/graph_tail_e2e.rs`): seed a `knows` FK self-link
  chain (1→2→3) and a `worksAt` FK link from people to companies (2→CompanyA,
  3→CompanyB, 1→CompanyX). Assert `?path=knows*,worksAt` from `_ids=1` returns
  `{CompanyA, CompanyB}` and **excludes** `CompanyX` (depth≥1 excludes the seed);
  a multi-hop tail (`knows*,worksAt,locatedIn`); a row-filter prune (an inactive
  intermediate person drops its company); `?path=worksAt,knows*` (`*` not first)
  → 400; `?path=knows*` (empty tail) → 400.

## Out of scope (explicitly deferred — already on the roadmap)

- **Multi-link / union recursive cores** (`knows,likes*` cycle-prefix,
  `knows|likes*` union-prefix). The core is exactly one self-link this slice.
- **Inverse links in the tail.** The tail is forward-only; the general chain
  resolver's inverse branch is not exercised here.
- **Positional / tail-target caller filters** (graph-aware filter addressing).
  `?param` binds to the seed/source type only.
- **Min-depth annotation, shortest-path/`/tree`, weighted edges** — separate
  follow-ons.

## Files

- Modify: `src/services/query-api/src/sql.rs` (extract `recursive_reach_cte`, add
  `compile_graph_reach_tail` + DuckDB wrapper).
- Modify: `src/services/query-api/src/handler.rs` (`GraphTailQuery`,
  `read_graph_reach_with_tail`, `BadGraphPath` variant).
- Modify: `src/services/query-api/src/http.rs` (`*` parse + dispatch in
  `get_graph_path`, `graph_error` arm).
- Create: `src/services/query-api/tests/compile_graph_reach_tail.rs`,
  `tests/graph_reach_tail.rs`, `tests/graph_tail_e2e.rs`.
- Modify: `src/services/query-api/BUCK` (three new test targets; e2e via
  `loom_fixture_test`, `duckdb = True`).
- Modify: `docs/FUTURE.md` and `docs/superpowers/specs/2026-06-06-loom-roadmap.md`
  (mark part B delivered; drop it from the deferred list).
