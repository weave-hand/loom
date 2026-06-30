# Wire-backed ControlPlane for query-api — governance reads over the engine wire (zero-postgres slice 2)

- **Date:** 2026-06-30
- **Area:** query
- **Register items:** promotes [[fut-query-api-wire-control-plane]] → mints [[road-query-api-wire-governance]]; records [[fut-auth-wire-resolve]] + [[fut-queue-wire-enqueue]] + [[fut-wire-governance-cache]]
- **Status:** spec (ready for a work agent to plan + build)

## North star

query-api reads the **governance metadata it needs** — ACL policy + ontology — **over the
engine wire**, not from its own Postgres connection. The engine becomes the *sole* reader of
the control-plane governance schema; query-api moves one decisive step toward being a pure
wire client. This is the governance-transport half of the zero-Postgres goal — distinct from
the write relocation that came before it.

## Current state

[[road-engine-serving-write-relocation]] (slice 1) made query-api's **library**
Postgres-free: writes go to the engine over `EngineControl` RPCs, and a `buck2 cquery` guard
keeps `control_plane_postgres` out of the library's direct deps. But the **binary** still
builds a concrete `PgControlPlane`: `query-api/src/main.rs` constructs
`service_runtime::control_plane(pool, …)` and injects it as `Arc<dyn ControlPlane>` so the
handlers can **read governance metadata** —

- **ACL:** `load_policy` (the input to `compile_select_with`), `policies_for`, `check`
  (`handler.rs`, `action.rs`) — the row-filter/column-mask/deny governance compiled into
  every read and the authorization on every action.
- **Ontology:** `get_type`, `resolve`, `links`/`links_to`, `get_action`,
  `vector_indexes_for`/`get_vector_index` — type/link/action/index resolution for serving.

The same `PgControlPlane` also serves **`Auth`** (session resolve, `bootstrap_admin`,
`auth_flight`) and query-api directly **`queue.enqueue`**s the GC job. So the binary holds a
Postgres pool for **three** distinct reasons: governance reads (ACL+ontology), auth, and the
GC enqueue.

The engine already holds the **full** `ControlPlane` + pool (`engine/src/main.rs:23`,
`EngineControlService`), and `EngineControl` is a **typed protobuf gRPC service** over the
UDS (`FlushTable`/`GcTable`/`WriteObject`/`OverwriteTable`/`CompactTable`). ACL core domain
types already derive `serde`; ontology domain types (`ObjectType`/`PropertyDef`/`LinkDef`/…)
**do not yet**.

This slice relocates the **governance reads (ACL + ontology)** to the wire. Auth and the GC
enqueue stay direct (their relocation is sequenced later), so this slice **does not yet
remove query-api's Postgres connection** — it establishes the governance-read transport that
makes that removal possible.

## Design

### Transport — typed per-read `EngineControl` RPCs

Extend the existing protobuf `EngineControl` service with unary RPCs covering **exactly the
reads query-api invokes** — no speculative surface:

- **ACL:** `PoliciesFor`, `Check` (the reads underlying `load_policy` + action
  authorization).
- **Ontology:** `GetType`, `Resolve`, `Links`, `LinksTo`, `GetAction`, `VectorIndexesFor`,
  `GetVectorIndex`.

Each RPC's **request** carries typed args (type/link/action names, `PageReq`, the subject,
the `Action`/`PolicyTarget`). Each **response** carries the result as a **serde-encoded
`core` domain payload** (e.g. an `ObjectType`, a `Vec<LinkDef>`, the policy set) rather than
a hand-modelled parallel protobuf type-system — the `core` types are the contract, and this
keeps the proto thin and the Rust boundary fully typed. ACL types already derive `serde`;
**ontology domain types gain `serde` derives** (additive, no behaviour change). The
engine-side handlers on `EngineControlService` delegate straight to its own
`cp.acl()`/`cp.ontology()`.

### query-api side — a wire-backed composite `ControlPlane`

Two concern adapters over the engine client:

- **`WireAcl`** — implements the `Acl` **read** methods query-api uses (`policies_for`,
  `check`, and whatever `load_policy` composes from) by issuing the RPCs above.
- **`WireOntology`** — implements the `Ontology` **read** methods (`get_type`, `resolve`,
  `links`, `links_to`, `get_action`, `vector_indexes_for`, `get_vector_index`).

These compose into a **`WireControlPlane`**: `acl()` and `ontology()` return the wire-backed
adapters; **`queue()` stays the direct Postgres queue** (the GC enqueue is not relocated this
slice); `catalog()`/`lineage()` are unused by query-api and guarded. The **write/define**
methods on `Acl`/`Ontology` that query-api never calls are `unimplemented`-guarded — this is
a **read-only governance client**, not a full control plane. `main.rs` injects
`WireControlPlane` where it injected `PgControlPlane` for governance; the pool it keeps is
now only for `Auth` + the GC enqueue.

### What this slice deliberately does NOT do

- **Does not remove query-api's Postgres connection** — `Auth` resolution
  ([[fut-auth-wire-resolve]]) and `queue.enqueue` for GC ([[fut-queue-wire-enqueue]]) still
  use it. The credential-free binary (the operational payoff — no DB creds in query-api) is
  reached only when those two follow-ons also land. This slice is the *transport*, and the
  largest/novel piece of, that arc.
- **Does not cache** wire reads ([[fut-wire-governance-cache]]) — every governed read still
  resolves policy/ontology per request, now over the wire; correctness first, latency
  amortization later.

### Decided (not open)

- **Scope = ACL + ontology governance reads** (not auth, not queue) — the novel transport;
  the rest is sequenced.
- **Typed per-read RPCs** extending `EngineControl` (not a generic read RPC) — consistent
  with the existing typed control surface; serde-payload responses avoid a parallel proto
  schema.
- **Read-only wire client** — write/define paths stay `unimplemented` (query-api authorizes
  then sends pre-authorized writes via the existing write RPCs; it never defines governance).
- **Connection stays** for auth + GC enqueue — stated, not hidden.

## Scope

In scope:

- New `EngineControl` governance-read RPCs (ACL `PoliciesFor`/`Check`; ontology
  `GetType`/`Resolve`/`Links`/`LinksTo`/`GetAction`/`VectorIndexesFor`/`GetVectorIndex`) with
  serde-payload responses; engine-side handlers delegating to its `ControlPlane`.
- `serde` derives on the ontology domain types the RPCs carry.
- query-api `WireAcl` + `WireOntology` + the `WireControlPlane` composite; flipping the
  handler/action/serving governance reads onto it; `main.rs` injection change.

Out of scope:

- **Auth over the wire** ([[fut-auth-wire-resolve]]) and **GC enqueue over the wire**
  ([[fut-queue-wire-enqueue]]) — the remaining steps to the **credential-free binary**.
- **Wire-read caching** ([[fut-wire-governance-cache]]).
- Catalog/lineage over the wire (query-api reads neither); any change to the read **data**
  path (still Flight SQL) or the write path (already relocated); multi-engine/TLS on the
  socket ([[fut-engine-wire-multi-tls]]).

## Testing

1. **Behaviour-preserving e2e:** the existing query-api governed-read, link-traversal,
   action, and vector-search e2e suites pass **unchanged** with query-api wired to
   `WireControlPlane` (ACL+ontology over the engine, queue direct) — the governed SQL,
   masking/row-filtering, action authorization, and 403/422 bodies are identical whether
   governance is read direct or over the wire.
- **RPC round-trips:** each new RPC round-trips its `core` domain payload engine↔client
  (`get_type` returns the same `ObjectType`; `policies_for`/`check` return the same
  decision; `links` the same `LinkDef`s).
3. **Governance parity:** a denied/masked read produces the **same** masked/filtered result
   over the wire as direct (ACL enforced from wire-fetched policy); an unknown type/action
   surfaces the same not-found.
4. **Read-only guard:** a `WireControlPlane` write/define call (which query-api must never
   make) fails loudly rather than silently no-ops, pinning the read-only contract.

## Risk

- **Governance correctness is the core risk** — wrong policy/ontology over the wire would
  mis-enforce ACL. Mitigated by serde-payloading the **same `core` types** (no lossy
  re-modelling), and by the behaviour-preserving e2e suites (1, 3) being the existing
  governance tests run against the wire-backed plane.
- **Per-request wire latency** (governance reads now cross the socket): bounded — the UDS is
  in-pod, the reads are small, and caching is a named follow-on ([[fut-wire-governance-cache]])
  if it bites.
- **Partial result** (connection kept for auth+queue) is a deliberate, stated slice boundary,
  not an oversight — the credential-free payoff is explicitly the [[fut-auth-wire-resolve]] +
  [[fut-queue-wire-enqueue]] follow-ons.
- Additive RPC surface + a new injected plane; the engine's own reads, the write path, and
  the Flight SQL data path are untouched, so blast radius is query-api's governance-read
  wiring.
