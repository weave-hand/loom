# Lineage read maturation — transitive closure + pagination

- **Date:** 2026-06-30
- **Area:** lineage
- **Register items:** promotes [[fut-lineage-closure]] + [[fut-lineage-pagination]] → mints [[road-lineage-read-maturation]]
- **Status:** spec (ready for a work agent to plan + build)

## North star

A caller can ask for the **full provenance** of a dataset — its entire upstream ancestry
or downstream descendancy, not just one hop — and every lineage read is **bounded**
(paginated). This matures the `Lineage` read surface in one pass, since closure and
pagination touch the same three methods.

## Current state

`Lineage` (`control-plane/core/src/lineage.rs:62`) has three reads, all already typed to
take `PageReq` and return `Page<T>` — but the implementations **ignore the page arg**
(`_page`, `postgres/src/lineage.rs:64,88,93`) and **return one hop**:

- `events_for(run, _page)` — all events for a run, unpaginated.
- `upstream(dataset, _page)` / `downstream(dataset, _page)` — `graph_step` over
  `lineage.event_dataset`: the datasets one event away (output→input for upstream,
  input→output for downstream), via `Page::from_full` (everything in one page).

So two gaps: reads are **unbounded** ([[fut-lineage-pagination]] — the `_page` seam exists
but is unhonored), and provenance is **one-hop only** ([[fut-lineage-closure]] — no
ancestry/descendancy). The graph model is per-event dataset co-membership
(`lineage.event_dataset(event_id, namespace, name, direction)`).

This pattern is already solved next door: query-api's `/graph` reachability
([[road-graph-reachability]]) walks a self-relation with a **depth-bounded `WITH RECURSIVE`
CTE**. Lineage closure is the same shape over `event_dataset`.

## Design

### Transitive closure — a `depth` param + cycle guard

`upstream`/`downstream` gain a `depth` parameter (the [[fut-lineage-closure]] shape):

- `depth = 1` reproduces today's one-hop (back-compatible default);
- `depth = N` returns the set reachable within `1..N` hops;
- `depth` is validated against a **max cap** (`LINEAGE_MAX_DEPTH` constant, tunable later)
  so a caller cannot request an unbounded walk.

Postgres implements it as a depth-bounded recursive CTE over `event_dataset` (mirroring the
`/graph` reachability CTE):

```sql
WITH RECURSIVE up(namespace, name, depth) AS (
    SELECT $start_ns, $start_name, 0
  UNION                                   -- UNION (not ALL): dedups, so re-run cycles terminate
    SELECT b.namespace, b.name, up.depth + 1
    FROM up
    JOIN lineage.event_dataset a
      ON a.namespace = up.namespace AND a.name = up.name AND a.direction = 'output'
    JOIN lineage.event_dataset b
      ON b.event_id = a.event_id AND b.direction = 'input'
    WHERE up.depth < $max_depth
)
SELECT DISTINCT namespace, name FROM up
WHERE NOT (namespace = $start_ns AND name = $start_name)   -- exclude the seed
ORDER BY namespace, name ...                                -- stable order for the cursor
```

`downstream` is the same with the `output`/`input` directions swapped. The **`UNION` dedup
is the cycle guard** (a node already in the working set is never re-added — re-runs that
form cycles terminate); the `depth` cap is the hard bound. The memory fake does the
equivalent BFS with an explicit `visited` set + depth bound, so both adapters satisfy the
same contract.

The result stays `Page<DatasetRef>` — the closure is a *set* of datasets (a per-dataset
min-depth annotation is a possible follow-on, [[fut-min-depth-annotation]]-adjacent, not
this slice).

### Pagination — honor `_page` on all three reads

Apply the established cursor convention (the one the other control-plane list reads use:
`PageReq { after: Option<Cursor>, limit: Option<u32> }` → `Page` with a next-cursor) to all
three reads, replacing `Page::from_full`:

- **closure / one-hop reads:** `ORDER BY (namespace, name)`, `WHERE (namespace, name) >
  after`, `LIMIT limit + 1` to detect a next page; the cursor encodes the last
  `(namespace, name)`. The stable order makes the cursor deterministic across calls.
- **`events_for`:** `ORDER BY` a stable event key (event ordering / id), same `after` +
  `limit + 1` cursor.

Pagination composes with the closure: the recursive CTE materializes the deduped set, then
the outer `SELECT` orders + windows it by cursor — so a large ancestry is delivered in
bounded pages.

### Decided (not open)

- **`depth = 1` is the default** so existing one-hop callers are unaffected; the param is
  additive.
- **`UNION` (set semantics) in the CTE** is the cycle guard — no separate visited column in
  SQL; the memory fake mirrors it with an explicit visited set.
- **Result is `DatasetRef`** (no depth annotation this slice) — keeps the contract stable.
- **Control-plane trait + adapters only** — no new external HTTP lineage endpoint (lineage
  reads have no external surface today; exposing the graph is a separate concern), so this
  slice is consumer-ready capability, not a wire.

## Scope

In scope:

- `depth` param on `upstream`/`downstream` (default 1) with a `LINEAGE_MAX_DEPTH` cap and a
  cycle guard, on both adapters (postgres recursive CTE; memory BFS+visited).
- Honoring `PageReq` (cursor + limit) on `events_for` / `upstream` / `downstream`, replacing
  `Page::from_full`, via the existing cursor convention.
- The `Lineage` contract test (testkit) extended for both: multi-hop closure (incl. a cyclic
  re-run graph that must terminate), depth-cap enforcement, and pagination (page boundaries,
  cursor round-trip, stable order) — run against both adapters.

Out of scope:

- **Run-grouped lifecycle stitching** ([[fut-lineage-stitching]]) — splitting a run's
  inputs/outputs across START/COMPLETE events; loom emits one terminal event today.
- **Type↔table layer-join** ([[fut-type-table-lineage-join]]) — joining type-named and
  table-named lineage nodes.
- **OpenLineage payload validation** ([[fut-openlineage-validation]]); per-dataset min-depth
  annotation; any external HTTP lineage-graph endpoint.

## Testing

`Lineage` contract tests (testkit, both adapters — memory pure-logic, postgres via
`loom_fixture_test`):

1. **Multi-hop closure:** a chain `A→B→C→D` (each an event with input/output) →
   `upstream(D, depth=3)` returns `{A,B,C}`; `depth=1` returns `{C}` only; `downstream(A,
   depth=3)` returns `{B,C,D}`.
2. **Depth cap:** the same chain with `depth` beyond `LINEAGE_MAX_DEPTH` is rejected/clamped
   (decide one — spec says **rejected** with a clear error) and never walks unbounded.
3. **Cycle terminates:** a re-run graph where `X→Y` and a later event `Y→X` →
   `upstream(X, depth=large)` terminates and returns the finite reachable set (no infinite
   loop), proving the `UNION`/visited guard.
4. **Pagination:** a fan-out with more upstreams than `limit` → successive pages with the
   cursor return every dataset exactly once in stable order, with the final page signalling
   no-next; `events_for` paginates a multi-event run the same way.
5. **Back-compat:** `upstream`/`downstream` with `depth=1` return exactly what the one-hop
   `graph_step` returned (the existing one-hop tests still pass).

## Risk

- The recursive CTE is the new SQL; mitigated by the `depth` cap (bounded work), the `UNION`
  cycle guard (test 3), and reuse of the proven `/graph` reachability pattern. Postgres
  compile-time `query!` keeps it schema-checked (refresh `.sqlx` via `tools/sqlx-prepare.sh`).
- Pagination changes the read shape; mitigated by the stable-order cursor (deterministic),
  the round-trip test, and `depth=1` back-compat pinning the one-hop callers.
- Capability built slightly ahead of an external consumer (lineage reads have no wire today);
  acceptable — it is the foundation a lineage-graph API/UI will need, and the contract is the
  durable artifact.
