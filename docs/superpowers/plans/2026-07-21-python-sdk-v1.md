# Python SDK v1 (`loom-sdk`) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship `loom_sdk` — sync + async Python clients over a shared sans-IO core covering loom's write path (Arrow IPC ingest, admin ontology JSON) plus verification reads, with a pydantic extra (`LoomModel`/`Identity`/`Link`, bootstrap-aware `apply`, `land_instances`) — implementing `road-python-sdk-v1` (spec: `docs/superpowers/specs/2026-07-21-python-sdk-v1-design.md`).

**Architecture:** All request-building/response-parsing logic lives in pure functions (`_core.py`, `_arrow.py`, `errors.py`); `Client`/`AsyncClient` are thin httpx transport shells with identical surfaces. The pydantic layer derives ontology payloads and Arrow schemas from class declarations. One e2e smoke drives the real `standalone` composite binary (embedded postgres, file:// warehouse) over real HTTP from the async client.

**Tech Stack:** httpx + pyarrow (core), pydantic v2 (extra), buck2 `python_library`/`python_test` on `//third-party/python:*`, hermetic CPython 3.13.

**Wire truth:** every task MUST read `docs/superpowers/plans/2026-07-21-python-sdk-v1-wire-reference.md` — JSON field names, routing, and semantics there are authoritative and verified against the tree; do not re-derive them.

## Global Constraints

- Two base URLs: ingest writes → `ingest_url`; reads + `/admin/*` → `query_url`; single-`url` form sets both. Bearer auth `Authorization: Bearer <token>`.
- Python → loom type mapping (locked): `int→long`, `str→string`, `float→double`, `bool→boolean`, `datetime.datetime→timestamp`, `datetime.date→date`; `X | None` → optional/nullable. Arrow: `long→pa.int64()`, `string→pa.string()`, `double→pa.float64()`, `boolean→pa.bool_()`, `timestamp→pa.timestamp("us")`, `date→pa.date32()`.
- Errors: `LoomError(status, message)` base → `AuthError`(401), `ForbiddenError`(403), `NotFoundError`(404), `ConformanceError`(422, `violations: list[Violation]`), `RequestError`(other 4xx incl. 400/409), `ServerError`(5xx). `Violation` fields: `column`, `reason`, `expected`, `found`, `rule` (None when absent). **Deliberate spec deviations (all recorded in the spec by Task 8):** the spec's `ValidationError`(400) is `RequestError` here (avoids colliding with `pydantic.ValidationError` in user code) and additionally covers 409; the spec's combined `AuthError`(401/403) is split into `AuthError`(401) + `ForbiddenError`(403) (the wire distinguishes them: 401 = bad/expired token, 403 = ACL/admin deny with empty or `"forbidden"` body).
- **Sanctioned extensions beyond the spec's literal surface** (wire-grounded, in scope): the `loom_sdk/models.py` DTO module; `datasets.land` params `model_gate`/`run_id` and `models.land` params `identity`/`mode`/`buckets`/`merge_engine` (all real wire params); `datetime.date → date` alongside the spec's `datetime → timestamp`; `OntologyDriftError` as the named drift failure (the spec's apply step 1 sanctions erroring on drift).
- Tests: stdlib `unittest` via prelude `python_test`, `remote_execution = RE_TEST_PROPS` (load from `//platforms:defs.bzl`) on EVERY python_test — the inplace-par shebang only resolves on RE (see CLAUDE.md "Third-party Python deps").
- Every commit: `git add` new files first, `buck2 run //tools:prek -- run --all-files` green (every hook incl. muntjac-check), Conventional Commit message + trailer `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>` (blank line before).
- Before ANY buck2 command in a shell: `export RES_OPTIONS="timeout:1 attempts:1"`.
- muntjac gaps found while re-locking → fix upstream (weave-hand/muntjac), not workaround; report first.
- Work branch `work/road-python-sdk-v1`; lease-check before pushes.
- SDK code style: type-annotated, docstrings on public API only, no third-party test deps (no pytest), no `print`.

---

### Task 1: package scaffold, errors, sans-IO core, client shells with login

**Files:**
- Create: `src/sdk/python/loom_sdk/__init__.py`, `loom_sdk/errors.py`, `loom_sdk/_core.py`, `loom_sdk/client.py`, `loom_sdk/aclient.py`
- Modify: `src/sdk/python/BUCK` (add `:loom-sdk` library + `:units` test target)
- Test: `src/sdk/python/tests/core_test.py`

**Interfaces:**
- Produces: `errors.py`: exception classes per Global Constraints plus `Violation` (a `dataclass`) and `raise_for_response(status: int, body: bytes, content_type: str) -> None` (parses the 422 JSON per the wire reference; plain-text bodies become the message; 403's empty body → message `"forbidden"`). `_core.py`: `PreparedRequest = dataclass(method: str, service: str, path: str, params: dict[str, str], headers: dict[str, str], content: bytes | None)` where `service` ∈ `{"ingest", "query"}`; `login_request(username, password) -> PreparedRequest`; `parse_login(body: bytes) -> str`; `resolve_base(service, ingest_url, query_url) -> str`. `client.py`: `Client(url=None, *, ingest_url=None, query_url=None, token=None, timeout=30.0)` with `.login(username, password) -> Self` (stores token), `.close()`, context manager; private `._send(prep) -> httpx.Response` applying base-URL routing + bearer header and calling `raise_for_response` on any ≥400. `aclient.py`: `AsyncClient` mirroring 1:1 (`async def login`, `aclose`, async context manager).
- Consumes: `//third-party/python:httpx`, `RE_TEST_PROPS` from `//platforms:defs.bzl`.

- [ ] **Step 1: failing tests** — `tests/core_test.py`: `PreparedRequest` routing (`resolve_base("ingest", ...)` picks ingest_url; single-url form sets both; missing needed URL raises `ValueError`); `raise_for_response` mapping for 401/403/404/422/400/500 (422 body from the wire reference example `{"violations":[{"column":"age","expected":"long","found":"string","reason":"type_mismatch"}]}` → `ConformanceError` with one `Violation(column="age", reason="type_mismatch", expected="long", found="string", rule=None)`); `login_request` shape (`POST`, service `"query"`, path `/auth/login`, JSON body); `parse_login(b'{"token":"t1"}') == "t1"`. Shell tests with `httpx.MockTransport`: `Client(url=...).login(...)` sends the right request and stores the token; subsequent `_send` carries `Authorization: Bearer t1`; same via `AsyncClient` with `httpx.MockTransport` (async handler) driven by `asyncio.run`. Run: `buck2 test --console none root//src/sdk/python:units` — expected FAIL (module not found).
- [ ] **Step 2: implement** the five modules exactly per Interfaces. `__init__.py` exports `Client`, `AsyncClient`, and the error classes; it must NOT import pydantic or `loom_sdk.pydantic`.
- [ ] **Step 3: BUCK** — add:
```python
python_library(
    name = "loom-sdk",
    srcs = glob(["loom_sdk/**/*.py"], exclude = ["loom_sdk/pydantic/**"]),
    visibility = ["PUBLIC"],
    deps = [
        "//third-party/python:httpx",
        "//third-party/python:pyarrow",
    ],
)

python_test(
    name = "units",
    srcs = glob(
        ["tests/*_test.py"],
        exclude = [
            "tests/imports_test.py",     # the infra acceptance test — its own :imports target
            "tests/pydantic_*_test.py",  # :units-pydantic (Task 5; needs the pydantic dep)
            "tests/e2e_*_test.py",       # :e2e (Task 7; needs the fixture env)
        ],
    ),
    remote_execution = RE_TEST_PROPS,
    deps = [":loom-sdk"],
)
```
(The exclude list keeps each test family on the target that carries its deps/env; later tasks rely on these patterns, so new unit files are `tests/<x>_test.py`, pydantic ones `tests/pydantic_<x>_test.py`, e2e ones `tests/e2e_<x>_test.py`.)
- [ ] **Step 4: run** `buck2 test --console none root//src/sdk/python:units` → Pass; `root//src/sdk/python:imports` still passes.
- [ ] **Step 5: commit** `feat(sdk): loom_sdk core — errors, sans-IO request core, sync/async shells with login`.

### Task 2: Arrow encoding + ingest writes (`datasets.land`, `models.land`)

**Files:**
- Create: `loom_sdk/_arrow.py`, `loom_sdk/models.py` (wire DTOs — plain dataclasses, no pydantic; `LandAck` and `ModelLandAck` live here from the start, Task 3 extends it)
- Modify: `_core.py`, `client.py`, `aclient.py`
- Test: `tests/arrow_test.py`, `tests/ingest_test.py` (new)

**Interfaces:**
- Produces: `_arrow.py`: `to_ipc(data: pa.Table | pa.RecordBatch | list[dict], schema: pa.Schema | None = None) -> bytes` (list[dict] via `pa.Table.from_pylist(data, schema=schema)`; RecordBatch wrapped into a Table; IPC **stream** format via `pa.ipc.new_stream`); `empty_ipc(schema: pa.Schema) -> bytes` (zero-row stream). `_core.py`: `land_dataset_request(schema, table, ipc: bytes, *, mode=None, buckets=None, model_gate: list[dict] | None = None, run_id=None) -> PreparedRequest` (service `"ingest"`, content-type header `application/vnd.apache.arrow.stream`, `X-Loom-Model` JSON `{"columns": model_gate}` when given); `land_model_request(type_name, ipc, *, identity=None, mode=None, buckets=None, merge_engine=None) -> PreparedRequest`; `parse_land_ack(body) -> LandAck(snapshot_id: int, dataset: str)`; `parse_model_ack(body) -> ModelLandAck(snapshot_id: int, type_name: str)` (JSON key is `type`). Shell namespaces: `client.datasets.land(schema, table, data, *, mode=None, buckets=None, model_gate=None, run_id=None) -> LandAck` and `client.models.land(type_name, data, *, identity=None, mode=None, buckets=None, merge_engine=None) -> ModelLandAck`; identical on `AsyncClient` (namespaces are tiny objects holding a client ref — one sync, one async class each).
- Consumes: Task 1's `PreparedRequest`/`_send`/errors.

- [ ] **Step 1: failing tests** — round-trip `to_ipc` (list[dict] → bytes → `pa.ipc.open_stream` → equal table); `empty_ipc` yields 0-row stream preserving field types (incl. `pa.timestamp("us")`); request shapes (path `/datasets/raw/trades`, params only when set, header presence/absence); ack parsing; MockTransport shell test: `client.datasets.land("raw","t",[{"id":1}])` returns `LandAck(snapshot_id=…)` and sent Arrow bytes decode to the input; a 422 response body raises `ConformanceError` with parsed violations. Run `:units` → FAIL.
- [ ] **Step 2–4:** implement; run `:units` → Pass; commit `feat(sdk): arrow ipc encoding + ingest land surface`.

### Task 3: verification reads

**Files:** Modify `_core.py`, `client.py`, `aclient.py`; Test `tests/reads_test.py`

**Interfaces:**
- Produces (all service `"query"`): `client.ontology.types() -> list[str]`; `client.ontology.type(name) -> TypeDetail` (dataclass mirroring the wire reference: `name`, `table: TableRef(schema, name)`, `identity: str | None`, `properties: list[PropertyView(name, ty, required, description)]`, `links: list[LinkView(name, from_type, to_type, cardinality, description)]` — `cardinality` normalized to lowercase, `from`/`to` JSON keys mapped to `from_type`/`to_type`; `links_to`; `description`); `client.datasets.list() -> list[DatasetEntry(schema, name, project, updated, kind, base)]`; `client.datasets.get(schema, table, *, as_of=None, as_of_snapshot=None) -> DatasetDetail(table, snapshot_id, snapshot_time, columns: list[ColumnView(name, ty, nullable)], kind, base)`; `client.datasets.preview(schema, table, *, limit=None) -> Preview(columns: list[str], rows: list[list[str]], sampled: bool)`. Mirrored on `AsyncClient`.
- Consumes: Tasks 1–2 plumbing; dataclasses live in a new `loom_sdk/models.py` (wire DTOs — plain dataclasses, no pydantic).

- [ ] **Step 1: failing tests** — parse each response shape from wire-reference example JSON (fixtures inline in the test); `as_of`+`as_of_snapshot` together → `ValueError` client-side; 404 → `NotFoundError`; MockTransport delegation for both shells on `ontology.type` + `datasets.preview`. Run → FAIL.
- [ ] **Step 2–4:** implement; Pass; commit `feat(sdk): ontology + dataset verification reads`.

### Task 4: admin writes

**Files:** Modify `_core.py`, `client.py`, `aclient.py`, `models.py`; Test `tests/admin_test.py`

**Interfaces:**
- Produces: `client.admin.define_model(payload: dict) -> str` (POST `/admin/models`, service `"query"`, returns the 201 body's `name`; payload passed through verbatim — dict-level API per spec) and `client.admin.define_link(payload: dict) -> None` (201 plain-text `"defined"`); plus typed builders in `_core.py` used later by pydantic: `model_payload(name, schema, table, identity, properties: list[tuple[str, str, bool]], description=None) -> dict` and `fk_link_payload(name, from_type, to_type, from_column, to_column, cardinality="One", description=None) -> dict` (cardinality validated ∈ {"One","Many"}; the default is `"One"` because an FK field on the declaring type points at exactly one target — matching the tree's canonical `LinkDef::fk("customer","Order","Customer",Cardinality::One,…)` example).
- Consumes: Task 1 plumbing.

- [ ] **Step 1: failing tests** — builder output matches the wire-reference JSON exactly (assert full dict equality, incl. `{"ForeignKey": {...}}` externally-tagged backing); MockTransport: 400 `{"error":"…"}` surfaces the inner message in `RequestError`; 409 → `RequestError` with status 409; both shells delegate. Run → FAIL.
- [ ] **Step 2–4:** implement; Pass; commit `feat(sdk): admin define_model / define_link surface`.

### Task 5: pydantic extra — declarations and mapping

**Files:**
- Modify: `src/sdk/python/pyproject.toml` (move pydantic from `dependencies` to `[project.optional-dependencies] pydantic = ["pydantic>=2.11"]`), re-lock + re-buckify
- Create: `loom_sdk/pydantic/__init__.py`, `loom_sdk/pydantic/_mapping.py`
- Modify: `src/sdk/python/BUCK` (`:loom-sdk-pydantic` library; `:units-pydantic` test)
- Test: `tests/pydantic_mapping_test.py`

**Interfaces:**
- Produces: `LoomModel(pydantic.BaseModel)` — subclasses declare `class Customer(LoomModel, table=("crm","customers"))`; class-kwarg `table` required (missing → `TypeError` at class creation). `Identity = Annotated[T, LOOM_IDENTITY]` marker (exactly one Identity field required — zero or two+ → `TypeError` at class creation). `Link(target: type[LoomModel], column: str | None = None)` used as `Annotated` metadata: `customer: Link[Customer]` (via `__class_getitem__` returning `Annotated[int, _LinkMarker(Customer)]`… the FK value type is the *target identity's* python type, resolved lazily at class-creation of the *declaring* class; unresolvable → `TypeError`). Derived class attributes (computed at definition time, stored on the class): `__loom_table__: tuple[str, str]`, `__loom_identity__: str`, `__loom_properties__: list[tuple[name, loom_ty, required]]` (FK fields included, named `column or target.__loom_identity__`), `__loom_links__: list[_LinkSpec(field, target, fk_column)]`. `_mapping.py`: `loom_type(annotation) -> tuple[str, bool]` (python type → (loom ty, required); unmappable → `TypeError` naming the field), `arrow_schema(cls) -> pa.Schema`, `model_gate(cls) -> list[dict]` (the `X-Loom-Model` columns list). Import guard: `loom_sdk/pydantic/__init__.py` raises `ImportError("install loom-sdk[pydantic]")` if pydantic is absent.
- Consumes: type mapping from Global Constraints.

- [ ] **Step 1: re-lock** — edit pyproject (pydantic → extra), run `./tools/pybuckify.sh`, then verify `grep -c 'pypi_package' third-party/python/BUCK` still ≥ 12 and `pydantic` rules still present (extras are in uv.lock). **If muntjac dropped the extra's packages**, STOP and report BLOCKED (dogfood wall — fix upstream per policy; fallback only if directed: keep pydantic in core deps and record the deviation). Run `buck2 test root//src/sdk/python:imports` (still green — it imports pydantic from the unchanged third-party target). Commit `build(sdk): pydantic becomes the [pydantic] extra`.
- [ ] **Step 2: failing tests** — a valid two-class declaration (Customer/Order per the spec's example) yields the expected `__loom_*__` values (FK field `customer: Link[Customer]` → property `("customer_id", "long", True)` and link spec (field `customer`, fk `customer_id`)); `arrow_schema` field types incl. timestamp; `model_gate` JSON; definition-time `TypeError` for: missing `table`, zero identities, two identities, unmappable annotation (`bytes`), `Link` to a class lacking identity, duplicate FK column without explicit `column=`. Run → FAIL (new `:units-pydantic` target: same shape as `:units` but `srcs=["tests/pydantic_mapping_test.py", "tests/pydantic_apply_test.py"]`-style explicit list, deps `[":loom-sdk", ":loom-sdk-pydantic"]`, RE-pinned).
- [ ] **Step 3: BUCK** — `python_library(name="loom-sdk-pydantic", srcs=glob(["loom_sdk/pydantic/**/*.py"]), deps=[":loom-sdk", "//third-party/python:pydantic"], visibility=["PUBLIC"])`.
- [ ] **Step 4–5:** implement; Pass; commit `feat(sdk): pydantic LoomModel / Identity / Link declaration layer`.

### Task 6: pydantic extra — `apply` and `land_instances`

**Files:** Create `loom_sdk/pydantic/_apply.py`; modify `loom_sdk/pydantic/__init__.py`, `client.py`, `aclient.py`; Test `tests/pydantic_apply_test.py`

**Interfaces:**
- Produces: `client.ontology.apply(*models: type[LoomModel]) -> ApplyReport(created: list[str], unchanged: list[str], links_created: list[str])` and `client.models.land_instances(instances: Sequence[LoomModel]) -> ModelLandAck` (all instances same class → else `ValueError`; builds `pa.Table` from instances via `arrow_schema` + `model_gate`-consistent columns, FK fields flattened to their fk_column name; POSTs `/models/{type}`). Mirrored async. `apply` per model, in the given order: (1) `GET /ontology/types/{name}`: on 200 compare (identity, properties as (name, ty, required) sets, table) — equal ⇒ unchanged; different ⇒ raise `OntologyDriftError(LoomError)` naming the first difference; (2) on 404: `GET /datasets/{schema}/{table}`; on 404 land `empty_ipc(arrow_schema(cls))` with `model_gate(cls)` as the `X-Loom-Model` header to `POST /datasets/{s}/{t}` (the verified zero-row bootstrap; the gate is REQUIRED so date/timestamp columns survive — inference rejects them); (3) `define_model` with the builder payload; (4) after all models: for each link spec whose name (`<from>_<field>`) is absent from the from-type's current `links`, `define_link` (`fk_link_payload`, cardinality `"One"`, `to_column` = target identity, `from_column` = fk column). Cardinality compare on reads is lowercase — normalize.
- Consumes: everything prior.

- [ ] **Step 1: failing tests** — MockTransport script asserting the exact request SEQUENCE for: fresh apply of Customer+Order (type-404 → dataset-404 → zero-row land with `X-Loom-Model` → define_model ×2 → define_link once with cardinality `"One"`, Customer before Order preserved); idempotent re-apply (type-200 matching → no writes, report.unchanged); drift (changed ty) → `OntologyDriftError`, no writes; `land_instances` produces decodable Arrow with `customer_id` column and hits `/models/Order`; mixed-class instances → `ValueError`. Both shells (sync test + one async variant). Run → FAIL.
- [ ] **Step 2–4:** implement (`_apply.py` is sans-IO: it yields a plan of `PreparedRequest`s + comparisons; the shells execute it — keeps sync/async single-sourced); Pass; commit `feat(sdk): bootstrap-aware ontology.apply + land_instances`.

### Task 7: e2e smoke against the real composite

**Files:**
- Create: `src/sdk/python/tests/e2e_smoke_test.py`; Modify: `src/sdk/python/BUCK` (`:e2e` target)
- Possibly create: a tiny `defs.bzl` in `src/sdk/python/` if env plumbing needs a macro

**Interfaces:**
- Consumes: the full SDK; `//src/services/standalone:standalone` binary; the `loom_fixture_test` env idiom (`src/control-plane/postgres/defs.bzl:42-56`).
- Produces: `python_test(name="e2e", …, remote_execution = RE_TEST_PROPS)` whose env carries `STANDALONE_BIN = "$(location //src/services/standalone:standalone)"`, `POSTGRES_BIN_DIR`/`POSTGRES_LD_LIBRARY_PATH`/`LOOM_PG_FIXTURE_SLOT_DIR` exactly as `loom_fixture_test` injects them (copy the `$(location)` expressions from `defs.bzl` — do NOT invent new ones), plus the panic-lint-free python side needs nothing else.

- [ ] **Step 1: wire the target without a cross-config rebuild** — the plan review pre-answered the investigation; use these verified facts (spot-check them, don't re-derive): the binary takes `LOOM_QUERY_API_BIND_ADDR` / `LOOM_INGEST_BIND_ADDR` (`standalone/src/main.rs:10-11,104-105`), **requires `LOOM_ENGINE_SOCKET`** (a UDS path; no default, `main.rs:99-102`) and **`LOOM_WAREHOUSE_URI`** (`standalone/src/lib.rs:262`, use `file://<tempdir>/warehouse`); `LOOM_PG_MODE=embedded` + `LOOM_DATA_PATH` derive the pg data dir and the `<LOOM_DATA_PATH>/pgrun` socket; map the fixture env `POSTGRES_BIN_DIR`→`LOOM_PG_BIN_DIR` and `POSTGRES_LD_LIBRARY_PATH`→`LOOM_PG_LD_LIBRARY_PATH` (precedent `standalone/tests/composite_e2e.rs:23-37`); embedded boot always runs migrations. **Admin bootstrap**: `loom create-admin --username <u>` subcommand (password on stdin, `main.rs:17-91` → `runtime/src/create_admin.rs`) — run `STANDALONE_BIN create-admin` as a second process with the same embedded env after the composite is up. **Cross-config hazard**: `src/sdk/PACKAGE` applies the Python cfg modifiers, so a bare `$(location //src/services/standalone:standalone)` from `:e2e` would reconfigure and REBUILD the whole Rust tree under a new cfg hash. Pin it: add a `configured_alias` in `src/sdk/python/BUCK` targeting `//src/services/standalone:standalone` on the default platform (in-tree precedent: `src/ui/e2e/BUCK:16-20` `:bundle-wasm`), and point `STANDALONE_BIN` at the alias. Verify with `buck2 cquery` (or by the build being a cache hit) that standalone resolves to the same configuration as a plain `buck2 build //src/services/standalone:standalone`.
- [ ] **Step 2: write the test** — `e2e_smoke_test.py` (unittest, one test method to keep boot cost single): pick two free ports; spawn `STANDALONE_BIN` (env per Step 1: addrs, `LOOM_ENGINE_SOCKET=<tempdir>/engine.sock`, `LOOM_WAREHOUSE_URI=file://<tempdir>/warehouse`, embedded-pg env) into a `tempfile.TemporaryDirectory` data path; poll an **unauthenticated** readiness signal until up (bounded ~60s — `POST /auth/login` returning 401 for a bogus user proves the router is serving; do not poll with the admin token, which doesn't exist yet); then run `STANDALONE_BIN create-admin --username admin` (password via stdin) with the same embedded env; then drive the **async** client end-to-end: `login("admin", …)` → `apply(Customer, Order)` (types created) → `land_instances([Customer(...)×2])` → `land_instances([Order(...)×2])` → re-`apply` (all unchanged) → `datasets.preview("crm","customers")` shows 2 rows → `ontology.type("Order")` shows the link → one deliberate conformance violation (raw `models.land("Order", [{"order_id": 1}])` missing a required column) asserting `ConformanceError` with `reason == "missing_required"`. Terminate the subprocess in `addCleanup` (SIGTERM, wait, kill).
- [ ] **Step 3: run** `buck2 test --console none root//src/sdk/python:e2e` (RE — postgres boots there as the non-root buildbuddy user, same as CI fixture tests). Expected Pass. Then the full sweep `buck2 test --console none -j 8 root//src/...` stays green.
- [ ] **Step 4: commit** `test(sdk): e2e smoke — real composite over HTTP from the async client`.

### Task 8: docs, register close, metric gate

**Files:** Modify `CLAUDE.md` (SDK paragraph in "Third-party Python deps" or a short new subsection: what `loom_sdk` is, where it lives, the two-URL model, the pydantic extra, how to run its tests), `docs/ROADMAP.md` (remove `road-python-sdk-v1`; `## devx` header stays), `docs/system-capabilities/` (record the capability — likely a new `python-sdk.md` + README index line, or extend `build-and-test.md` — follow the README's conventions), `docs/superpowers/specs/2026-07-21-python-sdk-v1-design.md` (correct the spec claims the build disproved or the plan deviated from: `define_model` does not bind-validate at define time; the admin surface lives on query-api; the error hierarchy — `ValidationError`→`RequestError` incl. 409, `AuthError` split into `AuthError`(401)+`ForbiddenError`(403); FK-link default cardinality One; convert the stale `[[road-python-build-infra]]` links → plain text).

- [ ] **Step 1:** docs + register edits; new deferrals discovered during Tasks 1–7 recorded as FUTURE items (`from:2026-07-21-python-sdk-v1-design`); `bash tools/docs.sh validate` OK.
- [ ] **Step 2: metric gate** — no `.rs` files should be touched (verify `git diff --name-only $(git merge-base HEAD origin/main)..HEAD | grep -E '\.rs$'` is empty — if Task 7's admin-bootstrap decision added Rust, run `loom-complexity diff`/`loom-duplication diff` for real against the merge-base and fix findings). Python isn't measured by the Rust tooling; note that in the report.
- [ ] **Step 3: final sweep** — `buck2 build -v0 -M none --console none root//src/... root//third-party/python/...`; `buck2 test --console none -j 8 root//src/...`; prek all-files; docs validate. Commit `docs(registers): close road-python-sdk-v1; record capability`.

*(PR opening is the controller's finishing step after the final whole-branch review.)*
