# Ontology-derived OpenAPI operations — slice 2 of self-documenting API

- **Date:** 2026-06-30
- **Area:** devx
- **Register items:** promotes [[fut-autogen-api-specs]] → mints [[road-autogen-api-specs]]; records [[fut-openapi-per-subject-catalog]] + [[fut-ingest-ontology-openapi]]
- **Status:** spec (ready for a work agent to plan + build)

## North star

A client or codegen tool that fetches `GET /openapi.json` sees **concrete per-type
operations** — `GET /objects/Customer` returning a typed `Customer`, not the opaque
`GET /objects/{type}` template — derived from loom's **live ontology**. The static
hand-written framework ([[road-openapi-self-documenting]]) documents the *shape* of the
API; this slice fills it with the *actual types* an operator has defined, so the document
is a faithful, current contract.

## Current state

[[road-openapi-self-documenting]] (PR #246) shipped the static framework. Both services
expose `GET /openapi.json` + `GET /docs` (Scalar). query-api's document
(`query-api/src/openapi.rs`) is built by `build_openapi() -> utoipa::openapi::OpenApi`
from `#[derive(OpenApi)]` over `#[utoipa::path]`-annotated handlers and `ToSchema` DTOs.
It returns a **value, not a constant — precisely so this slice can `.paths.extend(...)` /
`.components.schemas.extend(...)`** ontology-derived content (the documented hook).

The shared seam `service-runtime::with_openapi(router, doc)`
(`runtime/src/openapi.rs`) registers the bearer security scheme, then **bakes a clone of
`doc`** into the `GET /openapi.json` closure and into Scalar's `/docs` HTML. The document
is therefore **fully static — built once at startup**. The generic object surface appears
only as route *templates*: `get_object` (`GET /objects/{type}`), `get_linked`
(`GET /objects/{from}/links/{link}`), `post_action`.

The ontology read surface needed to enumerate types already exists on the `Ontology`
trait (`control-plane/core/src/ontology.rs`): `list_types(page)`, `links(name, page)`,
`get_type(name)`. Each `ObjectType` carries its `PropertyDef`s (name, base/logical type,
nullability) and identity. So the inputs to generate per-type operations are all present;
nothing reads them into the doc today.

## Design

### Freshness — per-request live generation

The ontology is **mutable at runtime** (types are defined via ingest model-binding /
`define_type` after boot), so a boot-time snapshot would go stale. `GET /openapi.json`
becomes a **dynamic handler** that regenerates the ontology-derived content on each
request:

1. clone the **static base** doc (`build_openapi()` — unchanged, still compile-time);
2. read the **live ontology** — `list_types` (paginated to completion) and, per type,
   `links`;
3. run the generator (below) → `(Paths, Schemas)`;
4. `.paths.extend(...)` + `.components.schemas.extend(...)` onto the clone;
5. serve as JSON.

`/openapi.json` is low-traffic (a docs/codegen endpoint), so a paginated ontology read +
a pure doc build per call is acceptable; no caching this slice. `/docs` (Scalar) is
pointed at the **`/openapi.json` URL** (rather than embedding a baked clone) so the
rendered UI reflects runtime types too.

### Seam change — a dynamic doc provider

`with_openapi` keeps its job: register the bearer scheme, mount `/docs`, and serve a
**static** `/openapi.json` for services with no dynamic content (ingest, unchanged this
slice). query-api needs the live merge, so the seam gains a way to supply a **doc
provider** — a closure `Fn() -> OpenApi` (async, capturing query-api's `ControlPlane`
ontology reader) — that `/openapi.json` calls per request. The static path is the
degenerate provider `|| build_openapi()`. The bearer scheme + base info stay applied to
whatever the provider returns.

(Exact shape — a `with_openapi_provider` variant, or `with_openapi` taking an enum of
static-doc-or-provider — is a plan-time call; the contract is "query-api's
`/openapi.json` is regenerated per request from the live ontology, `/docs` renders the
same live doc, ingest stays static.")

### The generator — `ontology_openapi(types, links) → (Paths, Schemas)`

A **pure** function (no I/O — the handler does the reads and hands it a snapshot), in
query-api, mapping an ontology snapshot to OpenAPI content. Per `ObjectType`:

- **Component schema** named for the type (e.g. `Customer`): an `object` schema whose
  properties are the type's `PropertyDef`s, each mapped by the **base/logical-type →
  OpenAPI schema** codec (below); nullable per the property; the identity property marked
  `required`.
- **`GET /objects/{Type}`** — a concrete operation (the `{type}` template specialized to
  the literal type name) whose 200 response is an object `{ "objects": [ <Type> ] }`
  (array of the per-type schema, the typed form of `ObjectsResponse`). Query params
  (filters, projection, pagination) carried from the generic handler's documented params.
- **`GET /objects/{Type}/links/{link}`** per declared link (`links(Type)`) — response
  typed as an array of the **target** type's schema.
- **Typed-insert action** `POST` operation with a **request body** schema derived from the
  type's writable properties (the insert shape `post_action` accepts), referencing the
  per-type component schema (or a write-variant of it).

The operations carry the bearer `security(...)` reference like the hand-written ones.

### Base/logical-type → OpenAPI schema codec

The core new logic: map each loom property type to an OpenAPI schema —

- string → `string`; integer kinds → `integer` (with `format`); float → `number`;
  boolean → `boolean`; timestamp/date → `string` + `format: date-time`/`date`;
- `vector(N)` → `array` of `number` (`minItems`/`maxItems = N`);
- nullability → the property's nullable flag.

This is the **inverse** of the ingest column→`PropertyDef` mapping
([[fut-ingest-model-inference]]'s logical-type vocabulary); the spec notes the shared
vocabulary as a reuse seam but the codec is self-contained here (ontology type →
OpenAPI), not a dependency on that unbuilt slice.

### Exposure — public, full catalog

`/openapi.json` + `/docs` stay **un-gated** (as today). The generated doc therefore
exposes the **full type catalog** to any caller. Accepted: a schema catalog is not secret
(it mirrors Foundry's ontology browsability), and **ACL still governs all actual data
access** — the document advertises operations a caller may still be denied at call time.
Per-subject catalog filtering (generate only the types a bearer can read, gating
`/openapi.json` behind auth) is recorded as [[fut-openapi-per-subject-catalog]].

## Scope

In scope:

- The pure `ontology_openapi` generator (types + links snapshot → `Paths` + `Schemas`) in
  query-api: per-type component schema, `GET /objects/{Type}`, per-link traversal read,
  and the typed-insert `POST` action body.
- The **base/logical-type → OpenAPI schema** codec (incl. `vector(N)` → bounded number
  array, nullability, identity-required).
- The dynamic `/openapi.json` (and live `/docs`) in query-api — regenerated per request
  from the live ontology — and the `with_openapi` seam change to accept a doc provider
  (static path unchanged for ingest).

Out of scope:

- **ingest per-type land operations** ([[fut-ingest-ontology-openapi]]) — generating
  ontology ops into ingest's `POST /models/{type}` `build_openapi()`; a sequenced
  follow-on (this slice is query-api only).
- **Per-subject filtered catalog** ([[fut-openapi-per-subject-catalog]]).
- **Richer prose from descriptions** ([[fut-ontology-semantic-descriptions]]) — optional
  description strings on types/properties/links don't exist yet; the generator uses
  structure only, and consumes descriptions when that lands.
- Derived/aggregate property docs, multi-hop `/graph` operations, codegen-client
  generation, response caching of the dynamic doc.

## Testing

`loom_fixture_test` (the live ontology requires postgres) + pure-logic unit tests for the
generator/codec:

1. **Codec unit:** each base type (string/int/float/bool/timestamp/`vector(N)`/nullable)
   maps to the expected OpenAPI schema; `vector(N)` is a bounded number array.
2. **Per-type operations generated:** seed a type with mixed-type properties + a link →
   the doc contains its component schema (correct property types, identity required), a
   `GET /objects/{Type}`, a `GET /objects/{Type}/links/{link}` typed to the target, and a
   typed-insert `POST` body.
3. **Liveness:** fetch `/openapi.json`, then `define_type` a new type, then fetch again →
   the new type's operations appear **without a restart** (the per-request regeneration).
4. **Static framework intact:** the hand-written operations + DTO schemas from
   [[road-openapi-self-documenting]] are still present and valid; the merged document
   passes OpenAPI validation; the bearer scheme is applied to generated operations.
5. **Full catalog public:** `/openapi.json` returns every defined type's operations with
   no token (un-gated), confirming the accepted exposure posture.

## Risk

- **Per-request doc generation** adds ontology reads + a doc build to `/openapi.json`;
  bounded by the endpoint being low-traffic and the read paginated. The static base is
  untouched and generation is additive + pure, so a generator bug can only add/garble
  ontology paths, never break the hand-written framework (test 4 pins this).
- **Type→schema mapping** is the new logic; pinned by the codec unit test (1) and the
  per-type test (2) across every base type. `vector(N)` is the one non-obvious mapping.
- **Catalog disclosure** is a deliberate posture (public schema, ACL-governed data), not
  an oversight; the least-disclosure alternative is captured as
  [[fut-openapi-per-subject-catalog]] for when a tenant needs it.
- Built on the shipped `build_openapi()` value-returning seam (#246) exactly as that slice
  anticipated, so the extension point is proven; query-api only, so ingest is unaffected.
