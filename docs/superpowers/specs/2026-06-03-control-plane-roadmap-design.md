# Design: Control-Plane Roadmap (trait-first, contract-tested)

> **Status:** roadmap / exploratory. This is the umbrella design for loom's control
> plane. The trait surfaces here are **provisional** — sketched loosely so the
> connections between concerns are visible. Each concern then gets its own
> spec → plan → implement cycle, during which its trait is pinned down. When a
> trait stabilizes, update it here.

## Goal

Build loom's **control plane** — the typed, transactional access layer over the
Postgres schemas described in `ARCHITECTURE.md` (`ducklake.*`, `ontology`,
`queue`, `lineage`, `acl`). It is a **library** the three services (Ingest,
Transform, Query API) consume — not a service itself.

The build methodology, applied per concern:

1. **Trait** — define the concern's trait surface (provisional, in `core`).
2. **Test plan** — enumerate the behaviors/edge cases the trait must guarantee.
3. **Test harness** — generic contract-test functions in `testkit`, backend-agnostic.
4. **Tests against traits** — write the contract suite; it must fail against an
   unimplemented adapter.
5. **Implementation** — make the in-memory fake pass, then the Postgres adapter
   pass the *same* suite.

## Approach: ports & adapters with shared contract tests

Chosen (over a single-crate / trait-as-module layout) because the methodology's
centerpiece — "tests written against traits" — requires the traits to be
implementation-agnostic enough that **two backends pass one identical suite**. An
in-memory fake gives fast, hermetic tests and a second implementation that keeps
the trait honest; Postgres gives the real thing.

### Crate layout

`src/control-plane/` (currently a lib scaffold) is restructured into a family of
workspace crates:

```
src/control-plane/
  core/      control-plane-core       traits, domain types, ControlPlaneError. NO I/O, no sqlx, no tokio runtime.
  testkit/   control-plane-testkit    generic contract-test fns over the traits; backend-agnostic.
  memory/    control-plane-memory     in-memory fake adapters (Mutex<…>); fast tests + local dev.
  postgres/  control-plane-postgres   sqlx adapters + migrations (one dir per schema) + .sqlx offline metadata.
```

Dependencies: `core` ← `memory`, `postgres`, `testkit`. Each adapter crate has a
`tests/contract.rs` that invokes `testkit` against its own adapter. The same
suite runs twice.

The pre-existing top-level `control-plane` lib scaffold (`add()`/`it_works`) is
replaced by these crates; there is no `control-plane` binary (the control plane
is a library).

### Async & object safety

All trait methods are `async`. To keep the aggregator's `&dyn Queue`-style
dynamic dispatch object-safe, traits use the `async_trait` crate (boxes the
returned futures). Adapters run on `tokio`.

## Error model

A single `ControlPlaneError` (`thiserror`) lives in `core`; every method returns
`Result<T, ControlPlaneError>`:

```rust
#[derive(Debug, thiserror::Error)]
pub enum ControlPlaneError {
    #[error("not found: {0}")]            NotFound(String),
    #[error("conflict: {0}")]             Conflict(String),   // optimistic / snapshot conflict
    #[error("unauthorized")]              Unauthorized,
    #[error("serialization: {0}")]        Serialization(String),
    #[error(transparent)]                 Backend(#[source] Box<dyn std::error::Error + Send + Sync>),
}
```

Adapters map native errors (sqlx errors, etc.) into these variants. **Contract
tests assert on variants, never on messages**, so both backends can satisfy them.

## Trait surfaces (provisional)

All `async`, all `-> Result<_, ControlPlaneError>`. Domain types
(`JobId`, `TableRef`, `TypeName`, `SubjectId`, `DatasetRef`, …) live in `core`.

```rust
// queue — graphile_worker_rs-shaped
trait Queue {
    async fn enqueue(&self, job: NewJob) -> Result<JobId>;
    async fn dequeue(&self, kinds: &[JobKind]) -> Result<Option<Job>>;   // SELECT … FOR UPDATE SKIP LOCKED
    async fn complete(&self, id: JobId) -> Result<()>;
    async fn fail(&self, id: JobId, retry: RetryPolicy) -> Result<()>;   // attempt count + backoff
    async fn heartbeat(&self, id: JobId) -> Result<()>;                  // for crashed-worker reaping
}

// catalog — DuckLake read surface. loom reads ducklake.*, never writes it directly.
trait Catalog {
    async fn current_snapshot(&self, t: TableRef) -> Result<Snapshot>;
    async fn snapshots(&self, t: TableRef) -> Result<Vec<Snapshot>>;
    async fn files(&self, s: SnapshotId) -> Result<Vec<FileRef>>;
    async fn schema(&self, t: TableRef, at: SnapshotId) -> Result<TableSchema>;
}

// ontology
trait Ontology {
    async fn get_type(&self, n: &TypeName) -> Result<ObjectType>;
    async fn list_types(&self) -> Result<Vec<ObjectType>>;
    async fn links(&self, n: &TypeName) -> Result<Vec<LinkDef>>;
    async fn resolve(&self, n: &TypeName) -> Result<TableRef>;           // type → physical DuckLake table
    async fn actions(&self, n: &TypeName) -> Result<Vec<ActionDef>>;
}

// acl
trait Acl {
    async fn check(&self, s: SubjectId, a: Action, t: PolicyTarget) -> Result<Decision>;
    async fn policies_for(&self, s: SubjectId, t: PolicyTarget) -> Result<Vec<Policy>>; // row/col predicates the Query API pushes into DF plans
}

// lineage — opaque OpenLineage payload + typed envelope for the fields we query
trait Lineage {
    async fn emit(&self, e: LineageEvent) -> Result<()>;
    async fn upstream(&self, d: DatasetRef) -> Result<Vec<DatasetRef>>;
    async fn downstream(&self, d: DatasetRef) -> Result<Vec<DatasetRef>>;
}

struct LineageEvent {
    run_id: RunId,
    event_type: EventType,
    ts: OffsetDateTime,
    inputs: Vec<DatasetRef>,
    outputs: Vec<DatasetRef>,
    payload: serde_json::Value,   // full OpenLineage event, stored opaquely (JSONB in pg)
}
```

These are deliberately loose. Real signatures (pagination, streaming dequeue,
filter predicates) are pinned during each concern's own cycle.

## The transaction seam (cross-concern unit of work)

`ARCHITECTURE.md`'s headline property: a single transaction can mutate a snapshot
*and* record lineage *and* enqueue downstream work. The control plane expresses
this with an aggregator:

```rust
trait ControlPlane {
    fn queue(&self)    -> &dyn Queue;
    fn catalog(&self)  -> &dyn Catalog;
    fn ontology(&self) -> &dyn Ontology;
    fn acl(&self)      -> &dyn Acl;
    fn lineage(&self)  -> &dyn Lineage;

    // Run a closure inside one unit of work. The closure gets a `Tx` exposing the
    // same per-concern operations; all of them commit together or roll back together.
    async fn transaction<F, T>(&self, f: F) -> Result<T>
    where
        F: for<'t> FnOnce(&'t mut Tx<'t>) -> BoxFuture<'t, Result<T>> + Send;
}
```

- **Postgres adapter:** `Tx` wraps a real `sqlx::Transaction`; per-concern ops issued
  against it share the transaction; commit on `Ok`, rollback on `Err`.
- **Memory adapter:** `Tx` stages mutations and applies them to the shared state on
  `Ok`, discards on `Err`, under a single lock for atomicity/isolation.

**Built fully in Phase 0** (per decision). Because there are no real concern
operations yet in P0, the Tx machinery is contract-tested against a **minimal
internal probe op** — a scratch table (pg) / counter (memory) that exists only to
verify the transaction semantics:

- commit makes staged writes visible;
- an error inside the closure rolls everything back (no partial writes);
- isolation: concurrent transactions don't observe each other's uncommitted state.

Real cross-concern atomicity (e.g. `lineage.emit` + `queue.enqueue` in one Tx) is
exercised for real in Phase 5, once ≥2 concerns participate. The probe op is
test-only and is removed or kept solely under `#[cfg(test)]`/a test schema.

> **Risk note:** the `Tx` lifetime/closure signature is the highest-churn part of
> this design. The `for<'t> FnOnce(&'t mut Tx<'t>) -> BoxFuture` shape may need
> revision once real adapters are written (sqlx transaction lifetimes are
> notoriously finicky). Treat it as provisional even relative to the rest.

## Test harness

`testkit` exposes generic `async fn`s — one suite per concern plus the Tx-semantics
suite — each taking a **fixture factory** that yields a fresh, empty adapter:

```rust
// sketch — exact bound pinned in Phase 0
pub async fn queue_contract<CP, Fut>(make_fresh: impl Fn() -> Fut)
where CP: ControlPlane, Fut: Future<Output = CP> { /* enqueue→dequeue, SKIP LOCKED, fail/retry, … */ }
```

- **fake fixture** → `make_fresh()` returns a brand-new in-memory adapter.
- **pg fixture** → `make_fresh()` boots an **ephemeral, hermetic Postgres** (see
  below), creates a **fresh database per test** (cheap on an already-running
  cluster, full superuser via `trust` auth), runs migrations into it, returns the
  adapter. No external database, no Docker, no env-provided URL.

### Hermetic Postgres fixture

The Postgres binary is a **buck2 build input**, not an ambient dependency: an
`http_archive` pulls a prebuilt server from
[`theseus-rs/postgresql-binaries`](https://github.com/theseus-rs/postgresql-binaries),
pinned per platform, and the `rust_test` receives its `bin/` directory via `env`.
The fixture then boots a throwaway cluster per test process and tears it down on
`Drop`. Because the binary is materialized by buck2, these tests are **hermetic
and run in the default CI path** — same as the fake tests.

```python
# //src/control-plane/postgres:postgres-bin  (hand-written, not reindeer-generated)
http_archive(
    name = "postgres-bin",
    urls = select({
        "ovr_config//os:linux": ["https://github.com/theseus-rs/postgresql-binaries/releases/download/<ver>/postgresql-<ver>-x86_64-unknown-linux-gnu.tar.gz"],
        "ovr_config//os:macos": ["https://github.com/theseus-rs/postgresql-binaries/releases/download/<ver>/postgresql-<ver>-aarch64-apple-darwin.tar.gz"],
    }),
    sha256 = select({ ... }),   # pin per platform
    # check the archive's internal layout — may need strip_prefix for a top-level dir
)

rust_test(
    name = "contract_pg",
    srcs = glob(["tests/**/*.rs"]),
    env = {"POSTGRES_BIN_DIR": "$(location :postgres-bin)/bin"},
    deps = ["//third-party:sqlx", "//third-party:tempfile", "//src/control-plane/testkit:testkit", ...],
)
```

```rust
// test-support: boots `postgres` on a unix socket in a tempdir; Drop kills it.
pub struct PgFixture { _data: tempfile::TempDir, socket: tempfile::TempDir, server: std::process::Child }

impl PgFixture {
    pub fn start() -> anyhow::Result<Self> {
        let bin = std::path::PathBuf::from(std::env::var("POSTGRES_BIN_DIR")?);
        let data = tempfile::tempdir()?;
        let socket = tempfile::tempdir()?;
        anyhow::ensure!(Command::new(bin.join("initdb")).arg("-D").arg(data.path())
            .args(["--no-locale", "--encoding=UTF8", "-A", "trust", "-U", "postgres"])
            .status()?.success(), "initdb failed");
        let server = Command::new(bin.join("postgres")).arg("-D").arg(data.path())
            .arg("-k").arg(socket.path())
            .args(["-c", "listen_addresses=",            // unix socket only
                   "-c", "fsync=off", "-c", "full_page_writes=off"]).spawn()?;
        // poll `pg_isready -h <socket>` with backoff before returning
        Ok(Self { _data: data, socket, server })
    }
    // libpq treats a host starting with `/` as a socket directory
    pub fn conn_string(&self, db: &str) -> String {
        format!("host={} user=postgres dbname={db}", self.socket.path().display())
    }
}
impl Drop for PgFixture { fn drop(&mut self) { let _ = self.server.kill(); let _ = self.server.wait(); } }
```

One cluster per test binary (boot once), one fresh `CREATE DATABASE` per test fn
for isolation; `fsync=off`/socket-only keep it fast. The fixture lives in
test-support (the `testkit` crate or a `postgres` test module).

### buck2 / CI integration

- Each crate gets a BUCK `rust_library`; contract suites are `rust_test` targets.
- **Both** the fake and the Postgres contract tests are hermetic `rust_test`
  targets and run in the default `//src/...` CI path — the fake needs nothing
  extra; the pg suite depends on the `:postgres-bin` `http_archive`.
- **RE caveat to verify in Phase 1:** the pg fixture spawns a `postgres` child
  process listening on a unix socket inside a tempdir. This should be fine under
  the BuildBuddy RE sandbox, but confirm it during Phase 1; if the sandbox blocks
  the spawned server, mark just that one `rust_test` `local_only` (it stays
  hermetic either way — the binary is still a buck input).
- **Third-party deps** (`tokio`, `async-trait`, `thiserror`, `sqlx`, `serde`,
  `serde_json`, `uuid`, `time`; plus test-only `tempfile`, `anyhow`) are imported
  via reindeer → `//third-party:*` and buckified (`./tools/buckify.sh`). The
  Postgres *server* binary comes from the `:postgres-bin` `http_archive`, not crates.
- **sqlx offline metadata** (`.sqlx/`) is committed so `control-plane-postgres`
  builds hermetically (compile-time-checked queries with no live DB at build time).

## Building blocks to evaluate

Same ecosystem, worth pulling in deliberately:

- **[`theseus-rs/postgresql-binaries`](https://github.com/theseus-rs/postgresql-binaries)**
  — the prebuilt Postgres server the hermetic pg fixture pulls via `http_archive`
  (P0/P1, above). Already load-bearing.
- **[`theseus-rs/rsql`](https://github.com/theseus-rs/rsql)** — a Rust SQL toolkit
  (Apache-2.0/MIT) whose reusable crates `rsql_driver` (a unified async
  driver/connection abstraction) and `rsql_drivers` (concrete drivers for **DuckDB**,
  PostgreSQL incl. *embedded*, SQLite, …) are strong candidates for when DuckLake /
  DuckDB enters in the **Catalog phase (P2)**:
  - Its **DuckDB driver** is the obvious way to talk to DuckLake/DuckDB without
    hand-rolling FFI; evaluate it as the `Catalog`/`postgres`-adapter substrate then.
  - Its **embedded Postgres** is built on the same `postgresql-binaries`, so it's an
    alternative to our hand-rolled fixture. Caveat: it *downloads* the binary at
    runtime by default — to stay hermetic we'd still feed it the buck2-materialized
    binary. Keep the hand-rolled fixture for P0/P1; revisit `rsql_driver` as a
    unifying abstraction only if it earns its keep at P2.

  Provisional — not a committed dependency; the call is made in the Catalog cycle.

## Roadmap

Each phase is its own spec → plan → implement cycle; this document is the umbrella.

| Phase | Concern | Deliverable |
| ----- | ------- | ----------- |
| **0** | Foundations | Crate split (`core`/`testkit`/`memory`/`postgres` skeletons); `ControlPlaneError`; the `ControlPlane`/`Tx` aggregator implemented in both adapters and contract-tested via the probe op; BUCK files + reindeer deps + sqlx offline; fake CI green. |
| **1** | **queue** | The **template** that proves the full loop end-to-end: trait → test plan → testkit suite → fake passes → pg adapter + `queue` migration + pg fixture passes. |
| 2 | catalog | DuckLake read surface (`ducklake.*`). |
| 3 | ontology | Types/links/actions; `resolve` type → physical table. |
| 4 | acl | Subjects, policy; predicate output for DataFusion plan rewrite. |
| 5 | lineage | Typed-envelope + opaque payload; upstream/downstream graph; **first real cross-concern `Tx`** (emit + enqueue atomically). |

### Open questions → phase that resolves it

Pulled from `ARCHITECTURE.md`; the trait sketches stay provisional precisely
because these aren't settled:

- Queue durability vs latency (LISTEN/NOTIFY + polling fallback, stuck-job reaper) → **P1**.
- Multi-writer ingest / DuckLake concurrency model → **P2** (catalog).
- Ontology authoring & migration → **P3**.
- ACL pushdown completeness (predicates that can't push into one scan) → **P4**.
- GC of orphaned Parquet → catalog-adjacent, flagged in **P2**, likely its own later cycle.
- Tenancy (partitioning `acl`/`ontology`) → surfaced in **P3/P4**.

## Non-goals (for this roadmap)

- The Quack-over-DataFusion server shim, the three services themselves, and
  DataFusion plan rewriting — the control plane is the library beneath them.
- Writing the `ducklake.*` tables directly (owned by the DuckLake client; loom reads).
- A fully typed OpenLineage model (payload stays opaque; typed envelope only).
- Ballista, GC implementation, and multi-tenancy mechanics (tracked as open questions).
