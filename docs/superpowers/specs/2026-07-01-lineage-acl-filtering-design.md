# Least-disclosure lineage reads — per-node ACL filtering

- **Date:** 2026-07-01
- **Area:** lineage
- **Register items:** promotes [[fut-lineage-acl-filtering]] → builds [[road-lineage-acl-filtering]]; **blocked-by [[road-dataset-naming-bridge]]**; tightens [[road-lineage-http-read]]
- **Status:** spec (ready for a work agent to plan + build, **after** the naming bridge lands)

## North star

Two subjects hitting the same `GET /lineage/…` endpoint see **different provenance
graphs** — each sees only the datasets it is allowed to read, and no more. A subject can
never learn, through a provenance read, that a dataset it cannot read exists, feeds
something, or connects two datasets it *can* read. Lineage becomes **least-disclosure**:
the same governance floor the object-read path already enforces, applied to the
provenance surface.

## Problem

[[road-lineage-http-read]] exposes `upstream` / `downstream` / `events` over HTTP to **any
authenticated subject, unfiltered** — a deliberate floor, taken because filtering needs a
`DatasetRef → ACL'd-object` mapping that did not exist. That mapping is now
[[road-dataset-naming-bridge]] (being specced in parallel). This slice consumes it to add
the filter.

The floor leaks. A `DatasetRef` names a physical table or ontology type that ACL governs;
today the provenance graph discloses those names — and their *connectivity* — to a subject
that has no `Read` grant on them. "You cannot read `t_salaries`, but you can see that it
feeds `t_headcount`" is a disclosure of `t_salaries`'s existence and its role. loom already
treats this class of leak seriously on the object path: `read_graph_reach` re-checks `Read`
on **every reached type** ("the leak-free guarantee", `handler.rs:764`) and omits a derived
property whose linked type is denied (`handler.rs:307`). Lineage must match that posture.

Two things make this non-trivial and are the crux of the design:

1. **Pagination.** The closure reads are keyset-paginated (`Page<DatasetRef>`, cursor over
   a stable `(namespace, name)` order — [[road-lineage-read-maturation]]). Dropping denied
   rows *after* the control plane has already windowed a page yields short/empty pages and
   a broken page-count contract.
2. **Closure.** The reads are a **transitive** closure. A denied *intermediate* node is not
   just a row to drop — if traversal still passes *through* it, the subject learns that the
   seed connects to that node's other neighbors, a fact that exists only because of the
   hidden node. Filtering the flat result set is not enough.

## Approach — filter at the query-api service layer

**Filtering happens in query-api, over resolved refs — not pushed into the control-plane
CTE.** Rationale:

- **Layering.** `core`'s `Lineage` "stores and serves provenance — it does not enforce
  anything" (`lineage.rs:4`); its reads take **no subject**. ACL (`Acl::check`) and the
  naming bridge are **service-layer** concerns — query-api already holds the `acl` handle
  (`AppState.cp.acl()`), and the bridge is explicitly a Step-3 service resolver that needs
  deployment context (`DatasetRef` doc, `lineage.rs:29-33`). Pushing an ACL/bridge
  predicate into the recursive CTE would invert that layering and drag a subject +
  per-subject policy into `core`.
- **Consistency.** Every existing ACL decision in loom lives in query-api's `handler.rs`,
  never in the control plane. This slice keeps that invariant.

The rejected alternative — a naive **post-filter of one already-windowed page** — is what
breaks pagination (crux 1) and only yields *skip* semantics, not *cut* (crux 2). The design
below solves both by making query-api **drive** the closure rather than forward a single
`depth=N` call: it walks the graph one hop at a time through the control plane's one-hop
read, ACL-gating each frontier, and materializes the whole (bounded) visible set before
windowing it. Because the visible set is a deterministic function of `(seed, depth,
subject)`, keyset windowing over it is stable and complete — no short pages.

This work **sequences strictly after [[road-dataset-naming-bridge]]** lands; it is a hard
prerequisite (there is no way to resolve a `DatasetRef` to an ACL'd target without it).

## Disclosure semantics (precise)

Define **readable**. For subject `U` and `DatasetRef r`, ask the bridge to classify `r`:

- **Internal(target)** — `r` names a loom `Table`/`Type`; `r` is *readable* iff
  `Acl::check(U, Read, target) == Allow`.
- **External** — the bridge classifies `r` as an ungoverned external datasource (e.g.
  `s3://…` source, per its internal-vs-external convention); `r` is *readable*
  (**default-allow**, justified below).
- **Unresolvable** — the bridge cannot classify `r` (a mapping gap for something that
  *should* be internal); `r` is **not readable** (fail-closed).

Any bridge or ACL **error** (infra fault, not a Deny) makes `r` not readable and surfaces
as a 500 — we never disclose on error.

**Upstream/downstream — cut, not skip.** The visible upstream closure of seed `S` for `U`
at depth `D` is the largest set `V` of `DatasetRef`s such that every `r ∈ V` is (a) readable
by `U`, and (b) reachable from `S` by a directed `output→input` path of length `≤ D` whose
**every intermediate node is itself readable by `U`**. Operationally: BFS from `S`,
expanding **only through readable nodes**, collecting readable nodes. A denied node is
**omitted from `V` and not expanded** — everything reachable *only* through it is therefore
never discovered. `downstream` is identical with the edge direction reversed.

This is **cut** (a denied intermediate terminates that branch), chosen over **skip** (omit
the node but keep walking through it) precisely because skip re-exposes the denied node's
edges: with skip, an exclusive ancestor `X` reachable only via denied `N` would still appear
in the flat set, disclosing that `X` is upstream of `S` — a provenance fact that holds
*only because of `N`*. Cut removes `X` too. The denied intermediate cannot leak via its
neighbors.

**Seed gating.** `upstream`/`downstream` return the empty page when the seed `S` is not
readable by `U` (the provenance *of* a dataset you cannot read is itself a fact about that
dataset). Empty — not 403/404 — so the endpoint is not a seed-existence oracle (empty is
indistinguishable from "no provenance"). The seed is excluded from results regardless
(existing contract).

**Events — redact refs, keep the envelope.** `events_for` filters *within* each event: a
denied `DatasetRef` is omitted from that event's `inputs`/`outputs` arrays; the event
envelope (`run_id`, `event_type`, `event_time`, `payload`... see Open questions on
`payload`) is retained. Redaction-within (not event-drop) is deliberate — it keeps
`events_for` a flat, non-row-dropping read, so it composes with the existing event-keyed
cursor **without any pagination interaction** (below). A caller only reaches `events_for`
with a `run_id` it already holds (its own `post_action` response), so run-envelope
disclosure is minimal.

**Why external is default-allow.** External `DatasetRef`s carry no loom-ACL'd data — there
is nothing for `Read` to gate — and they are typically the *source* leaves (`s3://…`) that
are the most valuable provenance breadcrumbs; omitting them would gut the read while
protecting nothing. It is also consistent with the bridge's mandate to "not reject
legitimate external lineage" (ROADMAP). Under cut semantics an external node is treated as
readable and so *passes traversal*; because external nodes are boundary/source leaves this
reveals no internal connectivity (Open questions revisits the adversarial case). A genuine
**Unresolvable** ref — an internal-looking dataset with no mapping — is the opposite case
and is fail-closed (denied + cut), so a bridge gap can never *widen* disclosure.

## Components

1. **Bridge resolver (consumed, not built here).** From [[road-dataset-naming-bridge]], the
   contract this slice needs: given a `&DatasetRef`, return one of
   `{ Internal(PolicyTarget), External, Unresolvable }`. This spec depends on that interface
   and does not redesign it; its own parity/contract tests belong to the bridge's spec.
2. **`LineageVisibility` filter (new, query-api — e.g. `src/services/query-api/src/lineage.rs`).**
   The service-layer governor. Given `(subject, depth, PageReq)` and a `LineageDeps { acl,
   bridge, lineage }`, it exposes `visible_upstream` / `visible_downstream` /
   `redact_events`. It owns the frontier BFS, the readability classification, the visited
   set (cycle guard, mirroring the CTE's `UNION`), the scan cap, and the final sort +
   keyset window.
3. **The three lineage HTTP handlers** ([[road-lineage-http-read]]'s routes) call
   `LineageVisibility` instead of forwarding the raw `Lineage` call. The route shapes,
   params (`depth`, `after`, `limit`), and JSON DTOs are unchanged — only the body between
   "parse" and "serialize" gains the filter.
4. **`LINEAGE_FILTER_SCAN_CAP` constant** (query-api). A hard bound on the number of
   distinct nodes the BFS may examine per request, so a subject that can read almost nothing
   cannot force an unbounded ACL fan-out over a huge closure. Sits alongside the existing
   `LINEAGE_MAX_DEPTH` depth bound.

## Data flow — pagination + filtering interaction

**`upstream` (downstream mirrors it):**

1. Parse `seed`, `depth` (validated by `check_depth`), `after`/`limit` (`PageReq`).
2. Classify `seed`; if not readable → return the empty page (seed gating). Done.
3. **Frontier BFS to `depth`, driven from query-api over the control plane's one-hop read**
   (`upstream(node, depth=1, …)`, draining its internal pages to get the full neighbor
   set): `visited = {seed}`, `frontier = {seed}`, `visible = []`. For each hop `1..=depth`:
   for each `node` in `frontier`, fetch its one-hop neighbors; for each neighbor not in
   `visited`, mark visited, classify it, and — if **readable** — add it to both `visible`
   and the next frontier; if **not readable**, drop it (not added to the next frontier →
   **cut**). Stop early when the frontier empties. Enforce `LINEAGE_FILTER_SCAN_CAP` on
   `|visited|`.
4. **Window.** Sort `visible` by the stable `(namespace, name)` order, then apply the
   keyset window: skip everything `≤ after` (via `decode_dataset_cursor`), take `limit`,
   and set `next` to `encode_dataset_cursor(last_emitted)` iff more remain, else `None`.

The pagination story: because `visible` is computed **before** windowing and is a pure
function of `(seed, depth, subject)`, the window is deterministic and every page is full
(save the genuine last page). Filtering never shortens a page — the drop happens during
closure assembly, not after windowing. The cursor encodes the last *emitted* readable ref,
so successive `after` requests reproduce the same ordered `visible` set and resume exactly.
The read is **stateless**: each page request re-runs the BFS (bounded by `depth` and the
scan cap; the closure is metadata, not row data). Trading recomputation for statelessness —
and for keeping the cursor a plain `(namespace, name)` keyset rather than a server-side
snapshot — is the accepted cost; a single-CTE optimization is an Open question.

**`events_for`:** unchanged control-plane read (event-keyed cursor, honored as-is); the
filter maps over the returned page, redacting denied refs from each event's `inputs`/
`outputs`. No row is dropped, so the page shape and cursor are untouched — **no pagination
interaction**. This is the payoff of redact-within over event-drop.

## Error handling

- **Bridge / ACL infra error** (not a Deny) → fail-closed: the ref is not readable and the
  request surfaces a **500** (`internal_error`). Never treat an error as Allow.
- **`LINEAGE_FILTER_SCAN_CAP` exceeded** → **422** (`"provenance closure too large to
  govern; reduce depth"`). Deterministic and leak-free — we never return a *partial* closure
  with a cursor (the cursor is a keyset over the *complete* visible set; a partial set has no
  valid resume point). The depth cap already bounds normal closures; the scan cap is the
  backstop against pathological fan-out.
- **Malformed `after` cursor** → 400 (existing `decode_dataset_cursor` → `Validation`).
- **Over-cap / zero `depth`** → 4xx (existing `check_depth`, unchanged).
- **Unknown seed** → empty page (unchanged; indistinguishable from denied-seed, preserving
  the non-oracle property).

## Testing

loom uses `rust_test` **integration** targets, never inline `#[cfg(test)]`. The filter is a
**query-api service concern** (not a `Lineage` trait method), so the primary vehicle is
query-api e2e through the HTTP surface; the bridge resolver's own testkit/memory/postgres
parity is owned by [[road-dataset-naming-bridge]]. This slice adds:

- **query-api e2e** (`loom_fixture_test`, lineage needs postgres; reuse
  `//src/services/query-api:e2e-support`), seeding a known governed provenance graph and
  distinct subjects via `subject_with_role`/`grant_read`:
  1. **Cut, not skip:** graph `A → N → X → S` with `U` granted `Read` on `A,X,S` but **not**
     `N`. `GET …/S/upstream?depth=3` returns **only `X`** (the readable node reachable
     through readable nodes); `A` is absent — proving the denied intermediate `N` cut the
     branch and did not leak `A` via connectivity. A subject granted `N` too sees `{X,A,N}`.
  2. **Flat-set diff between subjects:** an admin sees the full closure; a restricted subject
     sees the strict readable-and-reachable subset — same endpoint, same seed.
  3. **Seed gating:** `U` without `Read` on the seed → empty page (not 403/404); with `Read`
     → the governed closure. Denied-seed and unknown-seed responses are identical.
  4. **Pagination completeness:** a wide readable fan-out with more visible refs than
     `limit`, interleaved with denied refs → successive `after` pages return **every visible
     ref exactly once in stable order, each page full**, last page `next=null`; no page is
     short because of a drop (the regression the naive post-filter would cause).
  5. **Events redaction:** a run whose event has mixed readable/denied inputs → `GET
     /lineage/runs/{id}/events` returns the event with denied refs omitted from
     `inputs`/`outputs`, envelope intact, cursor/paging unchanged.
  6. **External default-allow:** an `s3://…` source ref (bridge → External) is present in a
     restricted subject's closure; an internal denied sibling is absent.
  7. **Fail-closed:** a scan-cap-exceeding closure → 422 (never a partial page); (if a
     bridge test-double can be faulted) a resolution error → 500, never an Allow.
- **Filter-unit e2e** for the `LineageVisibility` BFS against a stubbed bridge + memory ACL
  (pure-logic, RE-eligible): cut/skip distinction, cycle termination (re-run graph `X→Y`,
  `Y→X` terminates via the visited guard), depth-bound, scan-cap, and the sort+window keyset
  round-trip — the algorithm proven without a live postgres, mirroring how graph-reach logic
  is unit-tested.

## Non-goals

- **A graph-shaped (nodes + edges) response.** Still a flat `Page<DatasetRef>`; edges are
  never serialized (and cut semantics deliberately hide the edges through denied nodes).
- **Per-node depth annotation** — deferred with the capability ([[road-lineage-read-maturation]]).
- **Fine-grained row/column policy on provenance.** Only the coarse `Acl::check(Read)`
  decides a node's visibility; `RowFilter`/`deny_columns`/`mask_columns` do not apply to
  `DatasetRef` nodes (they govern *rows of a table*, not the table's provenance identity).
- **Pushing the readable-set into a single recursive CTE.** Kept as a service-layer BFS this
  slice (see Open questions); no `core`/bridge coupling into the SQL.
- **Caching the visible closure across page requests** — the read is stateless recompute.
- **Governing the emit/write path** — lineage is written transactionally by the engine, not
  over HTTP; this is read-side only.

## Open questions

- **BFS round-trip cost.** Frontier expansion issues one control-plane one-hop read per
  frontier node per hop (`K` calls/hop). Bounded by `depth` + scan cap and cheap for
  metadata graphs, but a wide closure is chatty. Future optimization: a **batch one-hop
  read** (`upstream_of(&[DatasetRef])`) or a **single recursive CTE that takes a readable
  predicate** — the latter needs the bridge to expose a *batch* resolver so query-api can
  pre-compute the readable-set and hand the CTE a `WHERE (namespace,name) IN (…)` filter.
  Deferred until the chatty path is shown to matter; record as a FUTURE follow-on.
- **External pass-through under cut.** External nodes are treated as readable and so pass
  traversal. If a deployment ever models an external node with *internal* downstreams, an
  external node could bridge two internal subgraphs — revisit whether External should *stop*
  traversal (visible but not expanded) under an adversarial threat model.
- **Event envelope disclosure.** Redact-within keeps an event whose datasets are *all*
  denied (envelope only). Acceptable because the caller already holds the `run_id`; but if
  `events_for` ever becomes reachable without prior `run_id` possession, reconsider
  dropping fully-denied events (accepting the events-pagination row-drop that entails).
- **`payload` redaction.** The opaque OpenLineage `payload` JSON may embed denied dataset
  names verbatim. This slice governs the typed `inputs`/`outputs`; whether/how to scrub the
  free-form `payload` (or omit it from the governed read entirely) is unresolved.
- **Depth semantics under cut.** With cut, a node's *min readable-path depth* can exceed its
  true graph depth (the short path ran through a denied node). The flat-set contract hides
  this, but a future per-node depth annotation would need to define depth as
  readable-path-length, not graph-length.
