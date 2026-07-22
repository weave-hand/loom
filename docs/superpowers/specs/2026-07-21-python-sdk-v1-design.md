# Python SDK v1 (`loom-sdk`) Design

> **Status:** shipped (`road-python-sdk-v1`, closed 2026-07-22). This spec made
> `road-python-sdk-v1` build-ready (promoted, with `road-python-build-infra`, from
> `fut-python-bindings`); a separate work agent wrote the implementation plan from
> it and built it, sequenced **after** `road-python-build-infra`, which provided the
> muntjac/uv dependency machinery and the `//third-party/python:*` targets this
> consumes. See `docs/system-capabilities/python-sdk.md` for the as-shipped
> capability writeup; the corrections below are places the build disproved or
> deliberately deviated from this design.

## Problem

loom's write path is only reachable today via curl/the UI: Arrow IPC to ingest
(`POST /datasets/{schema}/{table}`, `POST /models/{type}` —
`ingest/src/http.rs:139-140`, content type `application/vnd.apache.arrow.stream`) and
JSON to the runtime admin router (`POST /admin/models`, `/admin/links` —
`runtime/src/admin.rs:1884,1901`). The goal is writing real ingestion software in Python:
a first-party SDK that lands data and defines ontology from code, with pydantic as the
declarative front end for ontology creation.

## Decision (operator, 2026-07-21): hand-written dual-client SDK, pydantic as an optional extra

Operator decisions taken in the design session:

- **Hand-written client, not OpenAPI-generated.** The surface is ~15 endpoints, the
  interesting bodies are Arrow IPC (which generators can't express), and the DX lives in
  the hand-written layer. Drift is caught by the e2e smoke test, not codegen.
- **Sync + async from day one** — `Client` and `AsyncClient` with identical surfaces over
  a shared sans-IO core (pure request-build / response-parse / encode functions; the two
  clients are thin httpx transport shells — the openai/anthropic SDK pattern). No
  async-facade-over-sync or sync-wrapper-over-async.
- **pydantic is an optional extra** (`loom-sdk[pydantic]`): core deps are `httpx` +
  `pyarrow` only (Arrow IPC makes pyarrow unavoidable).
- **Name:** distribution `loom-sdk`, import `loom_sdk`.
- **Home:** loom monorepo, `src/sdk/python/` — buck2-built via
  `road-python-build-infra`, e2e-testable against the real services in-tree.

## Package layout

- `src/sdk/python/pyproject.toml` — `loom-sdk`, `requires-python = ">=3.13"`,
  deps `httpx`, `pyarrow`; `[project.optional-dependencies] pydantic = ["pydantic>=2"]`.
  This is the manifest `muntjac.toml` points at.
- `src/sdk/python/loom_sdk/` — `_core.py` (sans-IO request/response building, error
  mapping), `_arrow.py` (Table/rows → IPC stream bytes, IPC → Table), `client.py` /
  `aclient.py` (the two shells), `errors.py`, `pydantic/` (the extra; import guarded).
- BUCK: `python_library` `:loom-sdk` (httpx + pyarrow), `python_library`
  `:loom-sdk-pydantic` (`:loom-sdk` + pydantic) — the buck-side rendering of the extra —
  plus `python_test` targets (below).

## Core client surface (v1)

```python
client = loom_sdk.Client(url, token=...)                # co-deployed single host, or
client = loom_sdk.Client(ingest_url=..., query_url=..., token=...)
client = loom_sdk.Client(url).login(user, password)     # runtime /auth/login

client.datasets.land(schema, table, data, mode=None, buckets=None)  # → POST /datasets/{s}/{t}
client.models.land(type_name, data)                                 # → POST /models/{type}
client.admin.define_model(payload) / define_link(payload)           # plain-dict JSON bodies
client.ontology.types() / .type(name)                               # verification reads
client.datasets.list() / .get(s, t) / .preview(s, t)
```

- `data` accepts `pyarrow.Table`, `pyarrow.RecordBatch`, or `list[dict]` (converted via
  `pyarrow.Table.from_pylist`); encoded once in `_arrow.py` as an IPC stream.
- ingest and query-api are **two services**: the client resolves each call to the right
  base URL; the single-`url` form assumes co-deployment behind one host. **As shipped:**
  `client.admin.*` (`define_model`/`define_link`) is also a query-api call — the admin
  ontology-definition surface lives on query-api's router (`service_runtime::admin_routes`
  merged into query-api, not ingest), not on ingest as the endpoint list above might
  suggest by proximity to the ingest examples.
- Errors: `LoomError` base → `AuthError` (401/403), `NotFoundError` (404),
  `ConformanceError` (ingest 422, carrying the parsed `ViolationsBody` violations list),
  `ValidationError` (400), `ServerError` (5xx) — each carrying the server's message.
  **As shipped, this hierarchy changed** (recorded in the implementation plan's Global
  Constraints, not a build-time discovery): `AuthError` is 401-only, split from a new
  `ForbiddenError` (403) — the wire distinguishes a bad/expired token from an ACL/admin
  denial, so a combined class would lose that distinction; `ValidationError` is named
  `RequestError` instead (avoids colliding with `pydantic.ValidationError` in caller code)
  and additionally covers 409, not just 400.
- `AsyncClient` mirrors every method 1:1; both shells call the same `_core` functions.

## Pydantic layer (`loom_sdk.pydantic`)

The class declaration *is* the ontology type:

```python
class Customer(LoomModel, table=("crm", "customers")):
    customer_id: Identity[int]
    name: str
    tier: str | None = None

class Order(LoomModel, table=("crm", "orders")):
    order_id: Identity[int]
    customer: Link[Customer]      # FK-backed link; lands as customer_id
    total: float

client.ontology.apply(Customer, Order)
client.models.land_instances([Customer(customer_id=1, name="Ada")])
```

- **Type mapping:** `int`→int64, `str`→string, `float`→float64, `bool`→bool,
  `datetime`→timestamp; `X | None` → non-required property / nullable column. Unmappable
  annotations raise at class-definition time, not at `apply` time.
- **`Identity[T]`** (an `Annotated` alias) marks the identity property; exactly one
  required. **`Link[Other]`** declares an FK-backed link: it emits the FK property on
  this type *and* the `define_link` payload; instances hold the FK value. The FK
  property is named after the target's identity property by default (`customer:
  Link[Customer]` → `customer_id`); a second link to the same target must override the
  name via the `Link` metadata. **As shipped:** the FK link's cardinality defaults to
  `"One"` (an FK field on the declaring type points at exactly one target, matching the
  tree's canonical `LinkDef::fk(..., Cardinality::One, ...)` convention) — left unstated
  in this design and worth recording explicitly now that it's shipped.
- **`apply` is idempotent and bootstrap-aware.** loom defines models **over an existing
  table** (`DefineModelReq { name, table, identity, properties }`), so `apply`:
  1. reads the existing type (`GET /ontology/types/{name}`) — full match ⇒ no-op;
     incompatible drift ⇒ error (no silent migration — future work);
  2. if the backing table doesn't exist, lands a **zero-row Arrow IPC stream** with the
     model-derived schema to `POST /datasets/{schema}/{table}` to materialize it (if the
     landing path rejects zero-row streams, that's a loom-side fix or the SDK falls back
     to deferring table creation to the first `land_instances` — implementation plan
     verifies);
  3. `define_model` then `define_link` for each `Link` field, in dependency order across
     the passed classes.

  **As shipped, a correction to step 3:** `define_model` does **not** bind-validate
  physical conformance against the backing table at define time — the handler
  (`runtime/src/admin.rs::define_model`) builds an `ObjectType` and calls
  `ontology().define_type(otype)` directly, with no check against the table's actual
  Iceberg schema. Bind-style structural validation only happens on `POST /models/{type}`
  (ingest's typed-write path) — not on `POST /admin/models`. Zero-row bootstrap (step 2)
  still materializes a real table with the right physical schema before `define_model`
  runs, so `apply`'s own correctness doesn't depend on this — but a caller who
  hand-writes a mismatched `define_model` payload against an existing table today gets
  no define-time error for it.
- `land_instances` converts instances → `pyarrow.Table` (FK fields flattened) → the
  conformance-gated `POST /models/{type}`.
- The extra is import-guarded: `import loom_sdk` never imports pydantic;
  `import loom_sdk.pydantic` raises a clear error if the extra isn't installed.

## Testing

- **Unit `python_test`s** (mocked httpx transport via `httpx.MockTransport` — no network):
  sans-IO core (request shapes, URL routing between the two services, error mapping incl.
  422 violations), Arrow encoding round-trips, pydantic mapping (annotations → payloads,
  `Identity`/`Link` extraction, definition-time failures), both shells' delegation.
- **One e2e smoke test**: boots ingest + engine + query-api over the hermetic
  postgres/MinIO fixture (the `loom_fixture_test` env pattern; plumbing the fixture env
  into a `python_test` — or a small Rust harness that execs the Python — is an
  implementation-plan decision) and drives the full loop through the **async** client:
  `apply(Customer, Order)` → `land_instances` → `datasets.preview` / `ontology.type`
  read-back, plus one deliberate conformance violation asserting `ConformanceError`.

## Out of scope (v1)

- The governed read surface beyond verification reads (objects/links/graph/search),
  actions, transforms admin, lineage reads — future SDK slices.
- Ontology migration / schema-drift resolution (`apply` errors on drift).
- PyPI publishing and wheel building (in-tree consumption only; revisit when the API
  stabilizes).
- Streaming/chunked ingest, retries/backoff policy tuning.
