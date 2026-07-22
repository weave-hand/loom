# Build and test capabilities

This document records what build, test, CI, and developer-experience capability
exists in loom and the decisions behind it. It deliberately overlaps with
[`CLAUDE.md`](../../CLAUDE.md) (the build-system operating instructions — commands,
footguns, workflows) and [`docs/build-execution.md`](../build-execution.md) (the
RE-vs-local execution model and its cost accounting): those documents carry the
how-to; this one carries the capability inventory — what guarantees the build and
test estate provides, why each piece is shaped the way it is, and where the known
gaps are. Specs live under `docs/superpowers/specs/`; register history in
`docs/ROADMAP.md`, `docs/FUTURE.md`, and `docs/ISSUES.md`.

_As of 4861433b._

## Hermetic build, toolchains, and remote execution

The whole build is hermetic and pinned: a dated `buck2` release aligned with the
vendored prelude submodule, a pinned Rust nightly dist assembled into a full host
toolchain, a pinned python-build-standalone CPython, and every dev tool consumed
as a pinned, checksummed release artifact through `tools/BUCK` (reindeer, prek,
btd/supertd, jq, rust-code-analysis, lucidshark-duplo). No host Rust, Python, or
jq is required anywhere — locally, in CI, or on remote execution. Third-party Rust
comes through reindeer in non-vendored `http_archive` mode (sources are fetched at
build time, not checked in), with `iceberg` as the tree's single git dependency,
pinned to carry the arrow/parquet 58 migration so the whole tree sits on one arrow
major.

Compute runs on BuildBuddy remote execution. The key capability decision is
*placement*: builds always go to RE (the RE platform's `dockerUser` is the
non-root `buildbuddy` user — `platforms/defs.bzl`), while test *runs* land on
the local executor by default — buck2/tpx only dispatches test-run actions to
RE under `--unstable-allow-all-tests-on-re`, and there is no remote test-result
cache. Placement is therefore an invocation-level choice, not a per-target
attribute: non-root dev machines run hermetic-Postgres fixtures locally, and
root hosts (cloud sessions, where local `initdb` would fail) route test runs to
RE — the cloud buck2 shim injects the flag for `buck2 test` and CI passes it
explicitly in `buildbuddy.yaml`. The `loom_fixture_test` macro
(`src/control-plane/postgres/defs.bzl`) deliberately sets no `remote_execution`
profile; its job is injecting the fixture env (PG binaries, `libxml2`, MinIO,
the boot-throttle slot dir) so a new fixture test is wired correctly by
construction. `docs/build-execution.md` is the reference for the
materialization cost model (including why `buck-out` is not cached and why
cloud routines build with `-M none`).

The postgres adapter's SQL is verified at compile time: sqlx `query!` macros read
a committed `.sqlx` cache, and a dedicated `sqlx-cache-check` test re-describes
every cached query against a live hermetic schema in the normal test sweep, so
the cache cannot silently go stale. The cache regenerator (`tools/sqlx-prepare.sh`)
runs a cargo-mode check, which surfaced a buck2/cargo dependency-model mismatch —
crates buck2 supplies graph-wide must still be direct Cargo deps — fixed by making
`typetag` and `serde` direct deps of the postgres crate (#182, #195).

The RE worker environment itself is a capability: a custom RBE container image
(`tools/ci/rbe-browser/`, the flame-public base plus the `chrome-headless-shell`
runtime libraries, published to public GHCR and digest-pinned in
`platforms/defs.bzl`) lets the browser-driven UI e2e run for real on remote
executors instead of auto-skipping. A publish-time smoke test runs the actual
pinned browser against the freshly built image, so an incomplete library closure
can never reach the pin (spec:
`2026-07-02-ui-e2e-hermetic-rbe-image-design.md`).

loom also has hermetic **Python dependency machinery**, the Python analog of the
reindeer flow above: `//tools:muntjac` (weave-hand/muntjac, its first production
use) and `//tools:uv` are vendored prebuilt binaries, and root `muntjac.toml`
drives `uv.lock` → buck2-rule generation for `src/sdk/python/pyproject.toml` into
a generated `third-party/python/` tree (python 3.13 only, matching the hermetic
CPython; linux x86_64+aarch64, `manylinux_2_28` on both platforms since pyarrow's
cp313 wheels are 2_28-only and 2_28 still accepts other deps' older-tag wheels).
`tools/pybuckify.sh` is the `buckify.sh` analog (`uv lock` → `muntjac vendor` →
`muntjac buckify`, `--frozen` for the network-free CI/hook form), and the prek
`muntjac-check` hook mirrors `reindeer-check` — it re-runs `pybuckify.sh --frozen`
and fails on drift between the manifests and the generated tree. Python target
configuration is wired through scoped `PACKAGE` cfg modifiers
(`third-party/python/PACKAGE`, `src/sdk/PACKAGE`) rather than the root `PACKAGE`,
so Rust target configurations stay untouched. An acceptance `//src/sdk/python:imports`
`python_test` imports `httpx`/`pyarrow`/`pydantic` from `//third-party/python:*`
on RE, proving the generated tree actually resolves and builds; the `build-test`
action's CI build scope was extended to `//third-party/python/...` alongside
`//src/...` (the `affected` action needs no such change — it scopes to impacted
`//src/...` targets and third-party deps come along transitively). That test is
pinned to the RE executor (`remote_execution = RE_TEST_PROPS` from
`//platforms:defs.bzl`), since the prelude's inplace-par bootstrap bakes the
hermetic interpreter's absolute path from the par-build action's (RE) sandbox
into the generated entrypoint's shebang, which only resolves when the test
itself also runs on RE — see `fut-python-par-local-shebang`.

## Test infrastructure

Tests are `rust_test` integration targets only — buck2 never runs inline
`#[cfg(test)]` modules, and a prek hook (`no-inline-tests`) makes silently-dead
tests structurally impossible.

**Hermetic Postgres fixtures.** Fixture-backed tests boot real `initdb`/`postgres`
from a vendored `postgres-bin`. Two whole-suite failure classes were engineered
out at the fixture level: a cross-process file-lock (`flock`) slot semaphore
bounds concurrently-alive clusters so mass boots stop exhausting SysV semaphores
(#150, spec: `2026-06-22-fixture-boot-throttle-design.md`), and dynamic shared
memory was moved from `/dev/shm` to `mmap` inside the per-cluster tempdir so
SIGKILL'd teardowns stop leaking tmpfs segments (#187). On top of that,
`PgFixture::shared()` boots **one cluster per test process** (344 call sites
swept), keeping isolation at the database level via `fresh_db()`; cluster
lifetime is closed by `PR_SET_PDEATHSIG` plus an `atexit` reaper, and the old
`-j 8` slot-starvation workaround is obsolete (#298).

**Adapter contracts and shared harnesses.** The `testkit` crate holds
per-concern contract suites that both control-plane adapters (the in-memory fake
and postgres) must pass, so backend swappability is a tested property, not a
convention. Seeding is a small builder DSL (`ObjectType::build(...)`,
`ActionDef::build(...)`) in core, which deleted the tree's worst duplication
family — the top-6 census pairs, 91–127 lines each (#304). Cross-crate test
support lives in the `//src/testing` package: `:seed` (vector fixtures, landed-
table seeds, kNN asserts) and `:flight` (`spawn_engine_uds` with connect-retry
readiness, replacing most of the tree's sleep-based sync flakes), which cut the
vector-cluster duplication census from 102 pairs to 27 (#324). query-api's e2e
suites share their seed/router/ACL plumbing through the `:e2e-support` library.

**End-to-end layers.** Three complementary e2e layers exist. In-process `oneshot`
suites carry behavioral breadth. An over-the-wire layer boots the real routers on
a `TcpListener` via `service_runtime::serve` and drives them with `reqwest`,
proving the ingest → query-api vertical over an actual socket — happy path, deny,
and malformed-request status codes (#178, spec:
`2026-06-23-e2e-http-client-design.md`). The governed Arrow Flight export has a
masked-column e2e pinning the one place an advertised-schema/data-schema
divergence could surface: the masked column must be `Utf8` in both
`get_flight_info` and the streamed `do_get` batches, every value the `'***'`
literal (#264). At the top of the stack, `//src/ui/e2e:login` drives the real
served wasm bundle through a vendored headless Chromium with `fantoccini`
(render, bad-credentials, successful login → Explorer), fully hermetic on RE via
the custom RBE image (specs: `2026-07-02-ui-login-e2e-fantoccini-design.md`,
`2026-07-02-ui-e2e-hermetic-rbe-image-design.md`).

**Property tests.** proptest guards the seams where loom has demonstrably had the
bug class: round-trips for `RowFilter` and the lineage envelope (#22), and three
pure-logic suites at the parser/compiler/decoder seams — the SQL-compile
injection property (caller values never appear verbatim in emitted SQL,
placeholder count equals param count), decode-of-arbitrary-bytes never
panics/overallocates for the vector-index codec, and arbitrary caller strings
through path parsing and filter coercion (#318).

**Coverage.** A BXL pipeline (`tools/coverage.sh` over `tools/coverage/cov.bxl`)
measures the whole `//src` tree: it discovers every `rust_test`, applies an
instrumentation constraint modifier that normal builds can never trip, runs
fixtures locally and pure-logic tests on RE, and merges to per-crate and combined
lcov/HTML reports. See CLAUDE.md's Testing section for the operating footguns.

## Lint and quality gates

Production code carries the **whole** `clippy::pedantic` and `clippy::restriction`
groups, configured once on the toolchain, with a large reasoned allowlist; the
enforced core is the panic-safety/error set (`unwrap_used`, `expect_used`,
`indexing_slicing`, `panic`, `map_err_ignore`, …). Test code is exempted from the
panic-safety lints only, via the `loom_rust_test` wrapper. The `map_err_ignore`
debt taken on during adoption was fully paid down — all 18 production
`map_err(|_| …)` sites now carry their source errors, with no suppressions left
(#200; resolution log in `docs/error-handling-debt.md`).

prek hooks are the gate mechanism, spanning all three git stages: formatting,
clippy-all, file hygiene, `no-inline-tests`, register validation
(`docs-validate`), Conventional-Commits enforcement, reindeer drift
(`reindeer-check`), and pre-push build/test. The CI `lint` action runs exactly
these hooks, so local and CI enforcement cannot diverge.

## Continuous integration

CI is BuildBuddy Workflows, defined in `buildbuddy.yaml` — a deliberate
**replacement** of GitHub Actions, not a parallel system (spec:
`2026-06-23-buildbuddy-ci-workflows-design.md`). Three actions: `build-test`
(pushes to `main`; full build + test, `main` always fully green), `affected`
(PRs; build/test only impacted targets), and `lint` (prek on all events). The
runners are thin orchestrators co-located with the RE cluster: builds run with
`-M none` and `BUCK_PREFER_REMOTE`, VMs are snapshotted with `buck-out` excluded
from the runner's `git clean` so the warm buck2 daemon survives reuse, and setup
is one shared idempotent script (`tools/ci/buildbuddy-setup.sh`) carrying the
single `BUCK2_RELEASE` pin. Once the workflows were proven green, the superseded
GitHub Actions CI was deleted (#173); `release.yml` and the Claude bot stay on
GitHub Actions deliberately.

The `affected` action is a faithful port of target determination: btd/supertd
(vendored fork binaries) diff the base and head target graphs, and only
`root//src/...` targets the diff impacts are built and tested. The design
resolved the runner's constraints explicitly — no git-context env vars (the base
is `main` by construction of the trigger), `merge_with_base: false` so btd sees
the clean PR head, `git_fetch_depth: 0` for a merge base, and the supertd binary
built once and executed directly in a throwaway worktree so no second buck2
daemon ever runs.

## Code health, docs registers, and developer tooling

**Code-health routines.** Scheduled skills generate deterministic census
registers — `loom-complexity` (rust-code-analysis) and `loom-duplication`
(lucidshark-duplo), both rendered through the vendored jq so output is
reproducible — plus `loom-stpa` for the safety model. Companion `*-fix` skills
remediate the worst hotspot/duplication family under TDD and open review PRs,
proving themselves with a census diff.

**The pillar-idioms audit programme.** The censuses fed a systematic five-agent
audit of all service pillars (spec: `2026-07-02-pillar-idioms-audit-design.md`)
that landed as a coordinated campaign of behavior-preserving quality PRs: the
query-api governed-read spine (#299), one authoritative PG↔Arrow conversion
layer (#301), the Iceberg commit skeleton (#302), shared service bootstrap +
fail-loud config parsing (#309), query-api read-path consolidation (#310),
action write-path decomposition (#315), engine-wire dedup + structured error
classes (#317), ingest's typed `ApiError` (#313) and pure violation collectors
(#316), the control-plane adapter hygiene batch (#323), and a dead-path sweep
(#328). The programme's discipline — census-diff proof, byte-identical output
where claimed, explicitly whitelisted behavior changes — is itself the
capability: large-scale refactoring with the test estate as the safety net.

**Docs registers and the work pipeline.** Deferred/planned/defect work lives in
three parsable markdown registers with a validated grammar; `tools/docs.sh`
provides `validate`, `query`, and reconciliation, and is itself covered by
`tools/tests/docs_test.sh`. On top sits a planning/checkout pipeline
(spec: `2026-06-21-work-item-planning-checkout-design.md`): `loom-work-plan`
gets an item to a claimable, spec-on-disk state; `loom-work-checkout` claims it
through a server-side git mutex — an atomic create-only push of the `work/<id>`
branch, so two concurrent sessions can never build the same item — with
PR-lifecycle release and stale-claim reaping (claims whose PR merged are reaped
immediately, #115). `loom-docs-update` and `loom-docs-organise` close and
reconcile items so the registers stay the source of truth.

**Self-documenting HTTP API.** Both services publish OpenAPI at `/openapi.json`
and render it at `/docs` (Scalar, chosen over swagger-ui because it needs no
build-time asset download and so keeps the hermetic build intact), with a drift
guard that fails CI when a route is mounted but undocumented (#246). query-api's
document is regenerated per request from the **live ontology** — concrete
per-type read/link operations and property schemas, not opaque
`/objects/{type}` templates — so the served contract tracks runtime type
definitions without a restart (#270). Every generated operation is **tagged by
its type name**, so the docs UI shows one section per type holding its List
operation, its real callable actions, and its outbound link traversals; the
typed write is documented as the *actual* `POST /actions/{name}` op per defined
action — request schema derived from the action's step parameter lists (one
flat body, union across steps) via the new `Ontology::list_actions`, a
multi-step op tagged into every involved type's section, statuses following
the handler (`201` for every kind; see `#iss-action-kind-status`) — replacing
the former phantom `POST /objects/{Type}` (#344). The runtime's own surface is documented too:
`service_runtime` exports per-router-family OpenAPI **fragments**
(`auth_openapi` / `service_account_openapi` / `admin_openapi`, utoipa-annotated
handlers + `ToSchema` DTOs, secret fields never echoed outside the deliberate
login/mint-token returns), and each service merges exactly the fragments for
the routers it mounts — query-api all three, ingest auth + service-accounts —
so `/auth/*` and `/admin/*` appear in each service's document without a single
duplicated annotation (#344). The admin fragment now spans the full management
surface — link/action define+delete, grant list/revoke, role delete, user↔role
assignment — and query-api's document adds the type-detail and dataset reads,
all held to the same route-set drift guards (#346).

## Known gaps

- `#fut-binary-subprocess-smoke` — boot the built service binaries as
  subprocesses to also cover `main.rs` + `Config::from_env`.
- `#fut-ui-component-test-fixture` — per-component headless-wasm render harness
  (the unit-level complement to the login e2e).
- `#fut-sql-compile-snapshots` — insta snapshot testing for the SQL compiler's
  ~54 hand-maintained SQL literals.
- `#fut-testkit-contract-split` — decompose testkit's monolithic 3293-line
  contracts into named sub-contracts for failure attribution.
- `#fut-test-harness-residuals` — `#[traced_test]` standardization and the
  ingest router-support trio left out of the shared harness.
- `#fut-coercion-taxonomy` — one shared value-coercion taxonomy across the seven
  per-direction dispatch functions.
- `#fut-conformance-enum-consolidation` — consolidate the parallel
  `BindViolation`/`Violation` enums into core.
- `#fut-clippy-promote-pedantic` — promote the allowlisted cheap-mechanical
  pedantic lints to enforced.
- `#fut-service-runtime-error-idiom` — hoist ingest's `ApiError` idiom into
  `service_runtime` for all governance-fronted services.
- `#fut-create-admin-noecho-password` — hidden no-echo password prompt for
  `loom create-admin`.
- `#fut-openapi-per-subject-catalog` — per-subject filtering of the (currently
  public) generated OpenAPI catalog.
- `#fut-ingest-ontology-openapi` — ontology-derived land operations in ingest's
  OpenAPI document.
- `#fut-codehealth-reflect` — a routine mining remediation-PR outcomes and
  register trends for higher-level patterns.
- `#fut-stpa-vendored-jq` — migrate `loom-stpa` onto the vendored `//tools:jq`.
- `#fut-macro-panic-lint-hook` — prek hook catching `expect`/`unwrap` smuggled
  inside `macro_rules!` bodies past the panic-safety lints.
