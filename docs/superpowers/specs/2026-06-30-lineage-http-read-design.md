# Governed lineage read endpoint — viewing provenance over HTTP

- **Date:** 2026-06-30
- **Area:** lineage
- **Register items:** mints [[road-lineage-http-read]]; records [[fut-lineage-acl-filtering]]; depends on [[road-lineage-read-maturation]]
- **Status:** spec (ready for a work agent to plan + build)

## North star

A caller can **view a dataset's provenance over HTTP** — its upstream ancestry, its
downstream descendancy, and the events of a given run — through governed, authenticated,
paginated endpoints. This is the wire that turns loom's lineage from an internal
control-plane capability into something an operator (or a future UI) can actually look at.

## Current state

Lineage is captured and queryable **inside** the control plane but has **no external
surface**. The `Lineage` trait (`control-plane/core/src/lineage.rs`) offers
`events_for(run, page)`, `upstream(dataset, page)`, `downstream(dataset, page)` over
`DatasetRef { namespace, name }` and `RunId(Uuid)`. query-api already *hints* at the gap:
`post_action` surfaces the action's `run_id` in its response with a comment that a caller
can "locate its lineage via `Lineage::events_for`" (`query-api/src/http.rs:742`) — but
**there is no endpoint to call**. A grep confirms query-api registers no `/lineage` route.

Two things are converging here:

- **The capability is maturing.** [[road-lineage-read-maturation]] (planned) adds transitive
  **closure** (a `depth` param, `WITH RECURSIVE` CTE, cycle guard, `LINEAGE_MAX_DEPTH` cap)
  and honors **pagination** (`PageReq` cursor+limit) on all three reads — today
  `upstream`/`downstream` return one hop and all three ignore their `PageReq`. That slice is
  deliberately **control-plane trait + adapters only — no external HTTP endpoint**.
- **This slice is that endpoint.** It is the thin governed wire over the matured trait, so
  it **depends on** [[road-lineage-read-maturation]] landing first (a work agent sequences
  it after; the spec is written now to set direction).

## Design

### Routes (query-api, behind `require_auth`)

Three hand-written routes registered alongside the existing object routes, each a thin
adapter over one `Lineage` method:

- **`GET /lineage/datasets/{namespace}/{name}/upstream`** — `?depth=N&after=<cursor>&limit=<K>`
  → `Lineage::upstream`.
- **`GET /lineage/datasets/{namespace}/{name}/downstream`** — same params →
  `Lineage::downstream`.
- **`GET /lineage/runs/{run_id}/events`** — `?after=<cursor>&limit=<K>` →
  `Lineage::events_for` (the read that resolves the `run_id` `post_action` already returns).

The `{namespace}`/`{name}` path segments address a `DatasetRef`; `{run_id}` parses a
`RunId(Uuid)` (400 on a malformed UUID).

### Parameters

- **`depth`** (upstream/downstream): forwarded to the capability. Default **1** (the
  capability's back-compatible one-hop default); **hard-capped at `LINEAGE_MAX_DEPTH`** —
  an over-cap request is rejected by the capability (the endpoint surfaces that as a 4xx),
  so the wire cannot trigger an unbounded walk.
- **`after`/`limit`**: map to `PageReq` (the established cursor+limit convention), forwarded
  unchanged; the capability returns a stable-ordered page with a next-cursor.

### Responses (JSON, query-api paginated convention)

- upstream/downstream → `{ "datasets": [ { "namespace": .., "name": .. }, .. ],
  "next_cursor": <opaque|null> }` — the `Page<DatasetRef>` the capability returns,
  serialized. A **flat set** of datasets (no per-node depth — the capability returns a set,
  min-depth annotation is deferred *with* it; the endpoint does not invent it).
- events → `{ "events": [ <LineageEvent JSON>, .. ], "next_cursor": <opaque|null> }`.

### Governance — authenticated-only this slice

The endpoints sit behind `require_auth` (any **verified** `Subject`); they are **not**
ACL-filtered per node. Rationale: lineage nodes are `DatasetRef`s (table/type-named
provenance), and least-disclosure filtering — omitting datasets a subject cannot read —
requires resolving each `DatasetRef` to an ACL'd object type, which needs the
`DatasetRef→type` naming bridge ([[fut-dataset-naming-bridge]]) that does not exist yet.
Lineage is **provenance metadata**, and **ACL still governs all actual data access** (a
caller who sees that `t_pii` feeds `t_export` still cannot read either table's rows without
a grant). Per-node ACL filtering of the provenance graph is recorded as
[[fut-lineage-acl-filtering]], blocked on the naming bridge. This mirrors the
public-metadata / ACL-governed-data posture taken for the OpenAPI catalog
([[road-autogen-api-specs]]).

### OpenAPI

The three routes carry `#[utoipa::path]` annotations with `ToSchema` response DTOs, so they
appear in the self-documenting document and satisfy [[road-openapi-self-documenting]]'s
drift guard (every route must be documented). These are **static** hand-written routes —
orthogonal to the ontology-derived generation in [[road-autogen-api-specs]].

### Decided (not open)

- **Authenticated-only** (not per-node ACL-filtered, not admin-gated) — provenance is
  metadata; filtering is [[fut-lineage-acl-filtering]] once [[fut-dataset-naming-bridge]]
  lands.
- **All three reads** exposed — a thin complete wire over the matured trait; the events
  endpoint resolves the `run_id` `post_action` already returns.
- **Flat `DatasetRef` set** response (no per-node depth) — matches the capability's
  contract; per-node depth is deferred with the capability.
- **query-api hosts it** (the governed read service); read-only (no emit/write wire).

## Scope

In scope:

- Three `GET /lineage/...` routes in query-api over `Lineage::upstream`/`downstream`/
  `events_for`, behind `require_auth`.
- `depth` (default 1, capped) + `after`/`limit` (`PageReq`) parameter forwarding and
  validation (malformed UUID / over-cap depth → 4xx).
- JSON response DTOs (`{datasets,next_cursor}` / `{events,next_cursor}`) + `#[utoipa::path]`
  annotations satisfying the OpenAPI drift guard.
- e2e tests through the HTTP surface (below).

Out of scope:

- **Per-node ACL filtering** ([[fut-lineage-acl-filtering]]) — needs
  [[fut-dataset-naming-bridge]].
- **Per-node depth annotation** in the response — deferred with the capability's min-depth
  annotation.
- A **graph-shaped** (nodes + edges) response, multi-dataset batch queries, a lineage
  **UI**, and any **emit/write** wire (lineage is recorded transactionally by the engine,
  not via HTTP).
- The closure/pagination **capability itself** — owned by [[road-lineage-read-maturation]];
  this slice consumes it.

## Testing

`loom_fixture_test` end-to-end through query-api's HTTP surface (lineage requires postgres),
seeding a known provenance graph (e.g. `A→B→C`):

1. **Upstream/downstream traversal:** `GET …/C/upstream?depth=2` returns `{B,A}` (within
   the cap); `…/A/downstream?depth=2` returns `{B,C}`; `depth=1` returns the one-hop set.
2. **Depth cap:** `?depth=` beyond `LINEAGE_MAX_DEPTH` → a 4xx (never an unbounded walk).
3. **Pagination:** a fan-out wider than `limit` → successive `after`-cursor pages return
   every dataset once in stable order, last page has a null `next_cursor`.
4. **Run events:** take the `run_id` from a `post_action` response → `GET /lineage/runs/
   {run_id}/events` returns that run's events, paginated.
5. **Auth:** an unauthenticated request → 401 (`require_auth`); any verified subject → 200
   (no per-node filtering this slice).
6. **Bad input:** a malformed `run_id` UUID → 400; an unknown dataset → an empty page (not
   an error).

## Risk

- **Depends on an unbuilt capability** ([[road-lineage-read-maturation]]); the spec records
  the sequencing so a work agent builds the capability first. The wire itself is thin
  (parameter forwarding + serialization), so its risk is low once the capability is green.
- **Disclosure posture** (authenticated subjects see the full provenance graph) is a
  deliberate floor, not an oversight — [[fut-lineage-acl-filtering]] is the planned
  tightening, gated on [[fut-dataset-naming-bridge]]. ACL still governs the underlying data.
- **Unbounded-walk / large-page** risks are already handled *below* the wire by the
  capability's `depth` cap and pagination; the endpoint only forwards and validates, so it
  cannot bypass them (tests 2–3).
- Additive (new routes, read-only, no change to existing object/action paths), so the blast
  radius is the new `/lineage` surface only.
