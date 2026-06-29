# Self-documenting HTTP API via OpenAPI (utoipa + Scalar)

- **Date:** 2026-06-29
- **Area:** devx
- **Register items:** mints [[road-openapi-self-documenting]]; wires the slice-2 hook into [[fut-autogen-api-specs]]
- **Status:** spec (ready for a work agent to plan + build)

## North star

Every loom HTTP service publishes a machine-readable **OpenAPI** description of its
routes at `GET /openapi.json` and renders it at `GET /docs`, so the API is
self-documenting and client/codegen tooling can consume it. This slice builds the
**static framework** — the fixed routes, annotated and served — structured so a later
slice can inject **ontology-derived** per-type operations into the same document
([[fut-autogen-api-specs]]).

## Current state

No OpenAPI exists. Two axum routers carry the public surface:

- **ingest** (`src/services/ingest/src/http.rs`): `POST /datasets/{schema}/{table}`,
  `POST /tables/{schema}/{table}/compact`, and (incoming) `POST /models/{type}`
  ([[road-ingest-into-model]]).
- **query-api** (`src/services/query-api/src/http.rs`): `GET /objects/{type}`, the link
  + graph traversal reads, `POST /actions/{action}`, `POST /search/{type}/{index}`,
  `POST /maintenance/gc/{schema}/{table}`.

Both binaries already share the **`service-runtime`** crate
(`src/services/runtime`, `pub async fn serve(bind_addr, router)`,
plus the bearer-token `auth` middleware). That shared crate is the natural home for the
spec-serving seam.

## Design

### Dependencies (hermetic-safe)

Add via the standard reindeer flow (`Cargo.toml` → `cargo generate-lockfile` →
`./tools/buckify.sh`):

- **`utoipa`** — spec generation; `#[derive(OpenApi)]`, `#[utoipa::path(...)]`,
  `#[derive(ToSchema)]`.
- **`utoipa-scalar`** (with its `axum` feature) — a small embedded `/docs` HTML that
  loads the Scalar viewer **from a CDN at view time**. No build-time asset download, so
  the hermetic RE build is unaffected (this is why Scalar is chosen over
  `utoipa-swagger-ui`, whose `build.rs` fetches the swagger-ui dist at build time).

`utoipa` is proc-macro-bearing, so expect a `third-party/fixups/*/fixups.toml`
`[buildscript]` decision during buckify. **After the dep add, run the full
`buck2 test //src/...`** (not just per-crate) per the `reindeer update` silent-downgrade
guard in CLAUDE.md.

### Shared serving seam (`service-runtime`)

Add a reusable helper:

```rust
pub fn with_openapi(router: Router, doc: utoipa::openapi::OpenApi) -> Router
```

It mounts `GET /openapi.json` (serves `doc` as JSON) and `GET /docs` (the Scalar UI
pointed at `/openapi.json`). One implementation, both binaries call it — consistent and
opinionated. The bearer-token scheme from `auth.rs` is registered once here as an OpenAPI
`securityScheme` so documented operations can reference it.

### Per-service spec assembly — the ontology hook

Each service exposes a **function**, not a bare constant:

```rust
pub fn build_openapi() -> utoipa::openapi::OpenApi
```

It starts from a static `#[derive(OpenApi)] struct ApiDoc` (listing the crate's
`#[utoipa::path]` handlers + `components`) via `ApiDoc::openapi()`, and returns it.
Returning a value (rather than serving `ApiDoc::openapi()` directly) **is** the seam:
slice 2 calls `.paths.extend(...)` / `.components.schemas.extend(...)` to merge
ontology-derived per-type operations into the *same* document with no rework. This slice
returns the static document unmodified.

### Annotating the routes

- Each handler gets `#[utoipa::path(method, path, params(...), request_body = ...,
  responses(...))]` with typed status responses (200 / 4xx / 403 / 422).
- JSON DTOs and error bodies derive `ToSchema`: the object-row response shape, the
  `violations_json` body ([[road-ingest-into-model]]), the search request/response, the
  GC/compact acknowledgements, and the shared error envelope.
- **Arrow IPC bodies** (ingest land, `/models/{type}`) are binary: document the request
  body as `content_type = "application/vnd.apache.arrow.stream"` with a binary/string
  schema (utoipa supports a `format = Binary` body), since the payload is not JSON.
- Query parameters (`_ids`, caller filters, pagination cursor/limit) are described via a
  `params(...)` struct deriving `IntoParams`.

### Coverage

Both routers in this slice — ingest (~3 routes incl. `/models/{type}`) and query-api
(~8). The per-handler annotation is mechanical; the shared seam carries the rest.

## Testing

In-process `oneshot` `rust_test` (a plain `rust_test`, no fixture — the spec endpoint
touches no Postgres; build each router over the memory control-plane fake where an
`AppState` is required):

1. **Valid spec:** `GET /openapi.json` → 200, body **deserializes** as
   `utoipa::openapi::OpenApi` (or `serde_json` into an OpenAPI value), and `openapi`
   version + `info` are present.
2. **Path coverage:** every mounted route appears in the document's `paths` with the
   expected method + `operationId`.
3. **UI:** `GET /docs` → 200, `text/html`.
4. **Drift guard:** an explicit expected-`operationId` set asserted equal to the
   document's — so adding a route without documenting it **fails CI**. (utoipa does not
   introspect axum's route table, so this guard is what keeps "self-documenting" honest.)

All tests are `rust_test` integration targets, never inline `#[cfg(test)]`.

## Scope

In scope: the two new deps; the `service-runtime` `with_openapi` seam + security scheme;
`build_openapi()` + annotated handlers/DTOs for **ingest** and **query-api**; the tests
above; the `third-party/BUCK` regeneration.

Out of scope:

- **Slice 2 ([[fut-autogen-api-specs]]):** generating per-object-type/link operations and
  property schemas from the **live ontology** at runtime, merged through the
  `build_openapi()` seam.
- Offline/air-gapped UI assets (Scalar loads its JS from a CDN at view time; the spec
  JSON itself is always served locally) — bundling is a later option.
- Spec versioning/changelog, client SDK codegen, and documenting auth *flows* beyond
  declaring the bearer security scheme.

## Risk

- The main new surface is two third-party crates on the hermetic build; mitigated by
  choosing Scalar (no build-time download) and by running the full `//src` test after the
  dep add (silent-downgrade guard).
- Handler annotations are additive and compile-checked; a wrong/missing annotation is
  caught by the path-coverage + drift-guard tests rather than shipping a misleading spec.
- No runtime behavior change to existing endpoints — the spec/UI routes are additive, and
  `build_openapi()` returns a pure value with no request-path cost.
