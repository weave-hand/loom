# External `/search` vector kNN endpoint — design

**Status:** approved (brainstorming) — ready for `loom-work-plan` to land, then a work agent to plan/build.

**Slice of:** `fut-puffin-vector-index-ann` (the Puffin vector-index arc). Consumes the
named-index capability from [[road-ontology-vector-index-def]] (#223). The last
slice that turns the engine-internal vector search into an externally reachable,
governed feature.

## Problem

loom can build named vector indexes (Flat / IVF-Flat / HNSW) and the **engine** can
serve kNN over them (`engine_serving::vector_search(catalog, pool, table,
index_name, query, k)` → a 2-column `RecordBatch` of `(identity, _distance)`, cold
Puffin ∪ hot inline-delta merged), reachable today only via an internal Flight
`do_get` ticket. There is **no external surface**: an outside client cannot issue a
similarity search. The whole vector arc (column type → index build → named
declarations → engine search) is plumbing with no governed entry point.

Two query-time tuning knobs also remain unwired to any external caller: IVF
`nprobe` (`IvfFlatIndex::with_nprobe`) and HNSW `ef_search`
(`HnswIndex::with_ef_search`) exist as setters but are fixed at build-time defaults
because nothing passes a per-query value. This endpoint is their only natural home.

## Goal

A governed `POST /search/{type}/{index_name}` endpoint on **query-api** that returns
a ranked list of `{id, distance}` for a query vector, enforcing loom's ACL, and
exposing the optional `nprobe`/`ef_search` per-query knobs. A thin governed wrapper
over the existing engine search — query-api compiles no DataFusion (consistent with
its zero-DataFusion role).

## Non-goals / Out of scope (deferred to `fut-puffin-vector-index-ann`)

- **Index-level ACL/predicate pushdown** and over-fetch/refill-to-exactly-`k`. This
  slice post-filters the returned top-k against the subject's row-filters (secure,
  may return `< k`); it does not push predicates into the index nor over-fetch to
  refill.
- **Hydrated-object results.** The response is `{id, distance}` only; clients
  re-fetch properties via the already-governed `GET /objects/{type}?_ids=…`. No
  column projection/masking lives in `/search`.
- **Metrics beyond Cosine/L2** and non-`f32` element types.
- **Auto-rebuild / staleness** — unchanged; the endpoint searches whatever the
  newest built mirror row for `index_name` covers.

## Decisions (settled in brainstorming)

1. **Reference the index by name** — `POST /search/{type}/{index_name}`. Column /
   metric / kind / dim are resolved server-side from the index's mirror row (written
   from its ontology declaration at build). Cleaner than by-column now that indexes
   are named ([[road-ontology-vector-index-def]]).
2. **Response is ids + distances only.** Client re-fetches objects via `/objects`.
3. **Governance = coarse type Read gate + row-filter post-filter** (secure, may
   return `< k`). Column masking N/A (no properties returned).
4. **Expose optional `nprobe`/`ef_search`** per-query knobs, threaded
   request → ticket → the decoded index, applied only to the matching index kind.

## Design

### 1. Route & request (query-api)

A new route on the query-api router (`src/services/query-api/src/http.rs`), mirroring
the `post_action` JSON-body precedent:

```
POST /search/:type_name/:index_name
Authorization: Bearer <token>
{
  "query":     [0.1, 0.9, ...],   // required: f32 array, length must equal the index dim
  "k":         5,                  // required: 1..=K_MAX
  "nprobe":    16,                 // optional: applied only to an IVF-Flat index
  "ef_search": 100                 // optional: applied only to an HNSW index
}
```

`serde(deny_unknown_fields)`. `K_MAX` is a bounded constant (e.g. `1000`) so a single
request cannot ask for unbounded work. Metric and index *kind* are **not** in the
request — discovered server-side. The knob irrelevant to the resolved kind is ignored
(Flat needs neither).

### 2. Governance (query-api)

1. **Subject** from the bearer token (existing `Subject` extractor / `require_auth`
   middleware).
2. **Coarse gate:** resolve `type_name` to an ACL target; `acl.check(subject,
   Action::Read, target) == Deny → 403`. An unknown type also returns 403 (no
   existence leak), matching `/objects`.
3. **kNN:** call the engine for the top-k `{id, distance}` (step 4).
4. **Row-filter post-filter:** `load_policy(acl, subject, target)` → `row_filters`.
   If empty (unrestricted subject), skip — return the engine results directly.
   Otherwise issue a governed identity-projection read over *just the candidate ids*
   — `SELECT <id_col> FROM <relation> WHERE <id_col> IN (<top-k ids>) AND
   (<row_filters>)` via the existing `FlightSqlClient::execute` path (params inlined
   by `inline_params`) — and keep only surviving ids, preserving distance order.
   Never returns a forbidden row; may return `< k`. `<id_col>` is the type's declared
   identity column; `<relation>` its backing table.

### 3. query-api → engine (the kNN call)

query-api sends a `VectorSearchTicket` over the engine's Flight `do_get`
(the typed ticket path, distinct from the SQL `execute` path used for the
post-filter), and decodes the returned 2-column Arrow `RecordBatch`
(`identity: Int64|Utf8`, `_distance: Float32`) into the JSON result. No DataFusion in
query-api.

### 4. Engine-wire & engine changes (wiring the knobs + dim check)

- **`VectorSearchTicket`** (`engine-wire/src/flight.rs`) gains `nprobe: Option<u32>`
  and `ef_search: Option<u32>` (serde-defaulted; keeps `deny_unknown_fields`).
- **`engine_serving::vector_search`** (`engine-serving/src/vector_search.rs`) gains
  `nprobe`/`ef_search` params and, after `decode → Box<dyn VectorIndex>`, applies
  them via a new trait method before searching.
- **`VectorIndex::apply_query_knobs(&mut self, nprobe: Option<u32>, ef_search:
  Option<u32>)`** (`control-plane/core/src/vector_index.rs`) — default no-op;
  `IvfFlatIndex` applies `nprobe` (clamped `1..=nlist`, the existing `with_nprobe`
  logic); `HnswIndex` applies `ef_search` (clamped `≥1`, the existing
  `with_ef_search` logic); `FlatIndex` ignores both.
- **Dim validation:** the engine validates `query.len() == row.dim` (the mirror row
  carries `dim`) and returns a new `EngineServingError::DimMismatch` → query-api maps
  it to **400**.
- **`do_get_vector_search`** (`engine/src/flight.rs`) passes the ticket's
  `nprobe`/`ef_search` through to `vector_search`.

### 5. Response & errors (query-api)

```
200 OK   { "results": [ { "id": 42, "distance": 0.03 }, ... ] }   // ascending distance, ≤ k
400      malformed body, missing query/k, empty query, k out of 1..=K_MAX, query dim ≠ index dim
403      no Read grant (or unknown type)
404      no built index named {index_name} for {type}   ← EngineServingError::NoIndex
500      engine/internal
```
All candidates filtered out by row-filters ⇒ `200` with `"results": []` (the subject
may search; nothing is visible). The `id` value's JSON type follows the index's
identity kind (integer or string).

### Data flow

```
POST /search/document/by_sim { query:[…], k:5, ef_search:100 }
  → Subject (bearer)
  → acl.check(Read, document) ; Deny → 403
  → VectorSearchTicket{ schema, name, index_name:"by_sim", query, k:5, ef_search:Some(100) }
       → engine do_get → vector_search resolves mirror row "by_sim"
            (dim check; apply_query_knobs; cold Puffin ∪ hot delta) → top-k (id,distance)
  → load_policy(document).row_filters ; if any:
       SELECT id FROM main.document WHERE id IN (topk) AND (<row_filters>)  [FlightSqlClient]
       → keep survivors (≤ k), distance order
  → 200 { "results":[ {id,distance}, … ] }
```

## Error handling

- Body that is not a JSON object, or missing `query`/`k`, or `query` empty, or `k`
  outside `1..=K_MAX` → 400 before any engine call.
- `query.len() != index dim` → engine `DimMismatch` → 400.
- No built index named `index_name` for the type (no mirror row) → engine `NoIndex`
  → 404.
- Coarse `Read` deny, or unknown type → 403 (uniform, no existence leak).
- Any engine/internal fault → 500, logged server-side via the existing
  `internal_error` helper (no detail leaked in the body).

## Testing

All tests are `rust_test` integration targets; fixture tests use `loom_fixture_test`.
Reuse the query-api **`e2e-support`** library (`tests/e2e_support.rs`) for seed/HTTP/ACL
plumbing; extend it with a helper that lands a `vector(N)` type, declares a named
index (`define_vector_index`), and builds it, so the e2e can search.

- **Engine-serving:** `vector_search` knob passthrough — `nprobe`/`ef_search`
  accepted and applied to the matching kind (e.g. `nprobe=nlist` reproduces the exact
  oracle; an `ef_search` value is accepted and returns valid results); `DimMismatch`
  returns the new error; `apply_query_knobs` is a no-op on `FlatIndex`.
- **query-api e2e** (`e2e-support`): ranked `{id,distance}` for a seeded vector type
  searched by index name; subject without `Read` → 403; unknown type → 403; a
  `row_filter` that excludes the nearest hit drops that id (and may shorten the list);
  no built index for the name → 404; bad body / `k=0` / `k>K_MAX` / dim mismatch →
  400; `nprobe`/`ef_search` accepted (smoke). The **main test-infra lift** is seeding
  a built named index in the e2e harness (reuse the engine-serving build helpers) —
  call it out in the plan as the heaviest task.

## Affected components (for the implementation plan)

- `query-api`: new `POST /search/:type/:index_name` route + handler; request struct;
  governance (coarse gate + row-filter post-filter via `FlightSqlClient::execute`);
  the Flight ticket call + Arrow→JSON decode; `K_MAX` constant; error mapping.
- `engine-wire`: `VectorSearchTicket` `nprobe`/`ef_search`.
- `engine-serving`: `vector_search` knob params + dim check + `DimMismatch` variant.
- `engine`: `do_get_vector_search` passes the knobs.
- `core`: `VectorIndex::apply_query_knobs` (trait default + IVF/HNSW impls).
- Tests across engine-serving + the query-api `e2e-support` harness.
