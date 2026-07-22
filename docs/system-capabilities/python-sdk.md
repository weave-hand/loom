# Python SDK capabilities

`src/sdk/python/` ships `loom_sdk` (distribution `loom-sdk`, import `loom_sdk`) — a
hand-written, first-party Python client for loom's write path and verification
reads: sync `Client` and `AsyncClient` with identical surfaces over a shared
sans-IO core, plus an optional pydantic layer that turns a class declaration
into an ontology type. It is buck2-built on the muntjac/uv Python dependency
machinery (`docs/system-capabilities/build-and-test.md`), consumed in-tree
only — no PyPI publishing yet.

_As of b27064f7._

## Client shells and the two-URL model

`Client`/`AsyncClient` are thin `httpx` transport shells over one shared
sans-IO core (`_core.py` builds `PreparedRequest`s and parses responses;
`_arrow.py` encodes/decodes Arrow IPC; `errors.py` maps HTTP responses to
exceptions) — request-building and parsing live once in the core; the shells
duplicate only thin, mechanically-identical delegation methods (including the
`apply` orchestration loop, kept line-identical across the two files). loom is two services: ingest owns writes
(`datasets.land`, `models.land`), everything else — verification reads and the
`/admin/*` ontology-definition surface — is served by **query-api**, not
ingest (`service_runtime::admin_routes` is merged into query-api's router, not
ingest's). `Client(ingest_url=..., query_url=..., token=...)` resolves each
call to the right base; the single-`url` constructor form sets both for a
co-deployed host. `.login(username, password)` calls `POST /auth/login` and
stores the bearer token for subsequent requests.

## Error hierarchy

`LoomError(status, message)` is the base. The shipped hierarchy diverges from
the original design spec in ways the build proved out and the spec has since
been corrected to match:

- `AuthError` (401) and `ForbiddenError` (403) are **separate** classes, not
  one combined `AuthError` covering both — the wire distinguishes a bad/expired
  token (401) from an ACL/admin-role denial (403), and merging them would
  lose that distinction.
- `NotFoundError` (404), unchanged from the spec.
- `ConformanceError` (422) carries the parsed `violations: list[Violation]`
  (`column`, `reason`, `expected`, `found`, `rule` — the latter three `None`
  when the wire omits them).
- `RequestError` covers the remaining 4xx range (400 **and** 409) — named
  `RequestError` rather than the spec's `ValidationError` specifically to
  avoid shadowing `pydantic.ValidationError` in a caller's own code (a real
  collision risk once the pydantic extra is in play).
- `ServerError` (5xx).

## Write path (ingest)

`client.datasets.land(schema, table, data, *, mode=None, buckets=None,
model_gate=None, run_id=None)` and `client.models.land(type_name, data, *,
identity=None, mode=None, buckets=None, merge_engine=None)` both accept
`pyarrow.Table`, `pyarrow.RecordBatch`, or `list[dict]` (the last converted via
`pa.Table.from_pylist`), encoded once as an Arrow IPC **stream**
(`application/vnd.apache.arrow.stream`). `model_gate` sends the `X-Loom-Model`
conformance header; a 422 raises `ConformanceError` with the parsed violations.

## Admin writes and verification reads (query-api)

`client.admin.define_model(payload)` / `.define_link(payload)` take plain
dict JSON bodies passed through verbatim (`POST /admin/models` /
`/admin/links`, both on query-api). **`define_model` does not validate
physical conformance against the backing table at define time** — the
handler (`runtime/src/admin.rs::define_model`) builds an `ObjectType` from the
request and calls `ontology().define_type(otype)` directly; it performs no
`bind`-style check against the table's actual Iceberg schema. (The original
design spec claimed the ingest `bind` seam validated physical conformance
here — that claim was wrong and has been corrected; `bind`-style validation
only happens on `POST /models/{type}`, ingest's typed-write path, not on
`admin/models`.) `client.ontology.types()` / `.type(name)`,
`client.datasets.list()` / `.get()` / `.preview()` mirror the query-api
verification-read wire shapes 1:1, typed via `loom_sdk/models.py` dataclasses.

## Pydantic layer (`loom_sdk[pydantic]`)

An optional extra (`loom-sdk[pydantic]`, buck target `:loom-sdk-pydantic`):
`class Customer(LoomModel, table=("crm","customers")): customer_id:
Identity[int]; name: str` — the class declaration derives
`__loom_table__`/`__loom_identity__`/`__loom_properties__`/`__loom_links__` at
class-creation time (unmappable annotations, missing/duplicate `Identity`
fields, and FK-column collisions all raise `TypeError` there, not at `apply`
time). `Link[Other]` declares an FK-backed link; its default cardinality is
**`"One"`** (an FK field on the declaring type points at exactly one target —
matching the tree's canonical `LinkDef::fk(..., Cardinality::One, ...)`
convention; the design spec originally left this unstated and has been
corrected to say so explicitly).

**Self-referential and forward-referenced links are a recorded v1
limitation, not a bug**: `Link` resolution is eager, at the declaring class's
own class-creation, so a link target must already be a fully-defined
`LoomModel` subclass — declared textually before the class that links to it.
`parent: Link[Node] | None` inside `class Node(...)`, or any string/forward
annotation, raises a clear `TypeError` naming the constraint rather than
silently doing the wrong thing. Deferred: `fut-python-sdk-link-forward-refs`.

`client.ontology.apply(*models)` is idempotent and bootstrap-aware: per model,
it reads the existing type (200 + matching shape ⇒ unchanged; 200 + differing
shape ⇒ `OntologyDriftError` naming the first difference, no writes; 404 ⇒
lands a zero-row Arrow IPC stream — with the model's `X-Loom-Model` gate, so
date/timestamp columns survive inference — to materialize the backing table,
then `define_model`), then `define_link` for each not-yet-defined link.
`client.models.land_instances(instances)` converts same-class instances to a
`pyarrow.Table` (FK fields flattened to their column name) and posts to
`/models/{type}` under the same conformance gate as a raw `models.land` call.

## Testing and the e2e harness

Three buck2 test targets, all **RE-pinned** (`remote_execution =
RE_TEST_PROPS`) — the prelude's inplace-par bootstrap bakes the hermetic
CPython interpreter's absolute RE-sandbox path into the generated
entrypoint's shebang, so a locally-executed Python test cannot exec it; every
`python_test` in the tree needs the pin regardless of fixture status (see
CLAUDE.md's "Third-party Python deps" footgun note):

- `//src/sdk/python:units` — mocked-`httpx.MockTransport` unit tests for the
  sans-IO core, Arrow round-trips, error mapping, and both shells' delegation
  (no network).
- `//src/sdk/python:units-pydantic` — the pydantic declaration/mapping/`apply`
  layer, same mocked-transport style, plus `:loom-sdk-pydantic` deps.
- `//src/sdk/python:e2e` — one `unittest` method that boots the real `loom`
  standalone composite binary (embedded Postgres, `LOOM_WAREHOUSE_URI=file://`,
  engine + ingest + query-api + worker) as a subprocess, runs `create-admin`
  out-of-band, and drives the **async** client end to end over real HTTP: login
  → `ontology.apply` → ACL grant (raw HTTP — no SDK grant surface, see
  `fut-python-sdk-acl-admin` below) → `land_instances` ×2 → idempotent
  re-`apply` → `datasets.preview` / `ontology.type` read-back → one deliberate
  422 asserting `ConformanceError`. Wired via a `configured_alias` pinning the
  referenced `//src/services/standalone:loom` binary to
  `prelude//platforms:default` — the actual default target platform every
  `root//...` target resolves to — so pulling it into `src/sdk/PACKAGE`'s
  Python-cfg-modified tree doesn't reconfigure (and rebuild) the whole Rust
  tree under a different cfg hash.

This e2e run is also what surfaced a real ACL fact worth recording here: loom's
ACL is **deny-by-default even for the `admin` role** — holding `ADMIN_ROLE`
only gates `/admin/*` routes (`service_runtime::require_admin`), it carries no
implicit grants (`postgres::acl::check` is a pure grant lookup, no admin
bypass). A freshly bootstrapped admin subject cannot `land_instances` or
`preview` anything until roles/grants are set up via `/admin/roles*` — which
the SDK does not yet wrap (below).

## Known gaps

- `fut-python-sdk-acl-admin` — no typed `client.admin.roles`/`.grants` surface
  over `/admin/roles*`; programmatic ACL setup from Python is raw-HTTP-only.
- `fut-python-sdk-link-forward-refs` — self-referential and forward-referenced
  `Link` declarations are rejected with a clear error rather than supported.
