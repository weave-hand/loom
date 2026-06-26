# loom

**An open-source take on Palantir Foundry — a typed-object data platform with built-in lineage and governance: a Rust governance chokepoint over a DataFusion serving layer, with DataFusion for ingestion, all on an Iceberg + Postgres core.**

> ⚠️ **Status: pre-alpha.** The **control-plane library is built and hardened** — five concerns (queue, catalog, ontology, ACL, lineage) as ports-and-adapters, each with an in-memory fake and a real Postgres adapter against one backend-agnostic contract (Step 2 hardening complete: worker heartbeat, Tx isolation, MVCC delete contract, compile-time SQL, tracing). The **first services on top now run as networked services:** **ingest** lands data through a **DataFusion compute path** (Arrow → inferred Iceberg schema → multi-file Parquet → snapshot+lineage commit) and exposes a runnable **binary + plain-HTTP landing endpoint**; the governed **read path** resolves an ontology type, applies ACL, serves **typed-object JSON** via the **engine service** (DataFusion against the Iceberg mirror), and **traverses links** (FK- and join-table-backed, governed) behind a runnable **binary + HTTP endpoints**. An **MVP deploy** (apko/Wolfi OCI images + a Helm chart) ships both binaries. **Still missing:** Transform workers, ontology actions, distributed DataFusion, and an external SQL wire for third-party clients. The architecture is still framed as *exploratory*: the shape is sketched, several load-bearing decisions are flagged for hardening. It now *does* run as networked services over HTTP — but it's not a system for general use yet. If you're here to help design or build it, keep reading.

---

## Why loom exists

Palantir Foundry got a few things right that the rest of the data ecosystem mostly hasn't:

- **An ontology, not a table catalog.** Application teams work with `Customer`, `Order`, `Shipment` — typed objects with links between them — instead of joining raw tables.
- **Governance follows the data.** Row- and column-level ACLs, lineage, and audit aren't bolted-on dashboards; they sit in the query path itself.
- **Pipelines, queries, and apps share one substrate.** A transform that produces a dataset and an app that reads it talk about the same objects, with the same access rules.

The catch: Foundry is closed, expensive, and an all-or-nothing commitment. Loom is an attempt to build that *shape* — ontology + lakehouse + governance — from open components, self-hosted, in a stack a small team can actually operate.

## What loom is

Loom is a set of Rust services in front of a DataFusion serving layer and a Postgres database. Reads enter through the **HTTP query API** — the governance chokepoint that resolves the typed ontology to physical tables, applies ACL policy, and generates SQL — which it forwards over **internal Flight SQL** (`CommandStatementQuery`) to the **engine service**. The engine runs **[Apache DataFusion](https://datafusion.apache.org/)** against the Iceberg mirror, streaming Arrow IPC batches back; query-api decodes the batches and returns governed JSON. There is no embedded query engine in query-api itself. **[Apache Iceberg](https://iceberg.apache.org/)** (via `iceberg-rust`, catalog mirrored in Postgres) is the sole table format: data files live as Parquet on S3 or MinIO, and the mirror catalog (`iceberg_mirror.*`) is the Postgres-side MVCC projection the engine reads. DataFusion is also the ingestion and transform engine: bulk writes and queue-driven jobs land new Iceberg snapshots. The same Postgres holds the Iceberg mirror, the object/link ontology, ACL policy, the job queue, and lineage events in separate schemas.

For the full design rationale and open questions, see [`ARCHITECTURE.md`](./ARCHITECTURE.md).

## Foundry capabilities, mapped to loom

| Foundry concept                  | Loom equivalent                                                                                                  | Status         |
| -------------------------------- | ---------------------------------------------------------------------------------------------------------------- | -------------- |
| **Ontology** (objects, links, properties) | `ontology` schema in Postgres; resolved to physical Iceberg tables at plan time                          | Library built; **resolved to SQL on the read path** by the query API (objects + properties + links, via governed link traversal) |
| **Actions** (typed write-backs)  | Named actions defined alongside object types; governed at the HTTP query API and executed through the engine service, with loom owning the catalog commit so snapshot + lineage + enqueue stay atomic | **Built** (IcebergActionWriter, inline write seam, atomic snapshot+lineage commit) |
| **Pipelines / Code Repositories** | Transform workers pulling jobs from the `queue` schema; DataFusion plans against Iceberg snapshots             | Queue + worker library built; transform service not built |
| **Data Connection** (sources)    | Ingest service — DataFusion writes Parquet in bulk, commits a new Iceberg snapshot                              | **Built** (snapshot-commit, landing materializer, dataset→model binding, DataFusion multi-file compute path, runnable binary + HTTP landing); Transform workers pending |
| **Foundry SQL / Contour**        | HTTP query API (governance chokepoint) forwards SQL over internal Flight SQL to the engine service (DataFusion against the Iceberg mirror); returns typed-object JSON | **Read path built** (governed object reads + link traversal, typed-object JSON, Flight SQL wire, runnable binary + HTTP endpoints) |
| **Markings + project permissions** | `acl` schema — subjects, roles, row- and column-level policy compiled into the SQL the query API emits          | Library built; **enforced in generated SQL** (deny-by-default, row filters, column deny/mask) |
| **Data Lineage**                 | `lineage` schema with [OpenLineage](https://openlineage.io/) events; lineage commits atomically with snapshots   | Library built; **emitted on snapshot commit** (materializer, atomic with the catalog write); transitive lineage pending |
| **Compute backend** (Spark)      | DataFusion (single-node) for serving, ingestion, and transforms; [Ballista](https://datafusion.apache.org/ballista/) optional for distributed transforms | **Engine serving (DataFusion) built**; **DataFusion ingestion compute built**; transform compute (+ optional Ballista) pending |
| **Foundry Branching**            | Iceberg snapshots provide time-travel; named branches TBD                                                        | Open question  |

### What's deliberately *not* in scope (for now)

| Foundry capability          | Why loom is skipping it                                                                                          |
| --------------------------- | ---------------------------------------------------------------------------------------------------------------- |
| Workshop / Slate (app UIs)  | UI builders are a downstream concern. Build whatever UI you like on top of the HTTP query API.                   |
| Notebooks / Code Workbook   | Use Jupyter/Marimo/Hex against the Query API.                                                                    |
| AIP (LLM/agent suite)       | Out of scope. Loom is data infrastructure; an LLM layer can sit on top via the Query API and ontology metadata.  |
| Foundry's internal scheduler| Loom uses a Postgres job queue. If you need cron-style scheduling, run an external orchestrator that enqueues jobs. |

The principle: loom is the **data + governance + compute** core. The application layer above it is somebody else's product (or yours).

## How it works (60-second tour)

```
        Consumers / clients (BI tools, notebooks, app code)
                       │
                      HTTP
                       │
        HTTP query API  ── ontology + governance (Rust)
                       │   resolves to SQL, zero-DataFusion wire client
                       │   Flight SQL (internal, UDS)
                       ▼
        Engine — DataFusion serving
                       │   IcebergMirrorTableProvider + PgTableProvider
                       ▼
        Iceberg — storage
        Mirror catalog: Postgres (iceberg_mirror.* + ontology, ACL, queue, lineage)
        Data files: Parquet on S3 / MinIO
                       ▲
              writes (bulk) │ + new snapshots
        DataFusion — ingestion   ·   Transform workers (queue-driven)
```

- **One governed front door.** Every read enters the HTTP query API, which resolves the ontology, applies ACL, and compiles a request into SQL — so policy is never bypassable.
- **The engine service is the sole serving path.** It runs DataFusion against the Iceberg mirror (`IcebergMirrorTableProvider` for Parquet files, `PgTableProvider` for un-flushed inline rows) and streams Arrow IPC batches back over internal Flight SQL. query-api is a zero-DataFusion wire client — it inlines params, sends SQL, and decodes the result.
- **Postgres is the only stateful coordinator.** The Iceberg mirror catalog, ontology, ACL, queue, and lineage live there in separate schemas, so a single transaction can mutate a snapshot, record lineage, and enqueue downstream work.
- **Iceberg is the sole table format.** Snapshots + Parquet, with a MVCC mirror projection in Postgres so it can share transactions with the control plane.

For per-component detail, tradeoffs, and the list of decisions still up for debate, read [`ARCHITECTURE.md`](./ARCHITECTURE.md).

## Tech choices at a glance

- **Language:** Rust for the query API, engine, ingest, and transform services
- **Serving engine:** [Apache DataFusion](https://datafusion.apache.org/) in the engine service — `IcebergMirrorTableProvider` + `PgTableProvider`; internal Flight SQL wire
- **Ingestion / transform compute:** [Apache DataFusion](https://datafusion.apache.org/) (single-node), [Ballista](https://datafusion.apache.org/ballista/) (optional distributed)
- **Table format:** [Apache Iceberg](https://iceberg.apache.org/) — via `iceberg-rust` (`iceberg-catalog-sql` backend, Postgres mirror)
- **Internal query wire:** [Arrow Flight SQL](https://arrow.apache.org/docs/format/FlightSql.html) (`CommandStatementQuery`) between query-api and the engine service
- **Control plane:** PostgreSQL (single database, schema-separated: `iceberg_mirror`, `ontology`, `acl`, `queue`, `lineage`)
- **Job queue:** [graphile_worker_rs](https://github.com/leo91000/graphile_worker_rs)–style queue using `SKIP LOCKED` + `LISTEN/NOTIFY`
- **Lineage:** [OpenLineage](https://openlineage.io/) events
- **Object storage:** S3-compatible (S3, MinIO, R2, …)
- **Build system:** [Buck2](https://buck2.build)

## Project status & roadmap

Three steps, tracked in the documentation registers — [`docs/ROADMAP.md`](./docs/ROADMAP.md) (committed work), [`docs/FUTURE.md`](./docs/FUTURE.md) (deferred ideas), and [`docs/ISSUES.md`](./docs/ISSUES.md) (known defects); the original narrative roadmap is archived at [`docs/superpowers/specs/2026-06-06-loom-roadmap.md`](./docs/superpowers/specs/2026-06-06-loom-roadmap.md). `main` stays green.

1. **Control-plane library — ✅ delivered.** Five concerns as ports-and-adapters under `src/control-plane/` (`core` traits + domain types, `memory` fake, `postgres` adapter, `testkit` contracts, `worker`): **queue** (with a worker and `await_jobs`), **catalog** (Iceberg mirror read surface), **ontology**, **acl**, and **lineage**. Each runs against one backend-agnostic contract on both the in-memory fake and real Postgres. A cross-concern `Tx` seam makes `emit` + `enqueue` atomic.
2. **Harden the control plane.** Correctness and contract gaps catalogued in [`docs/superpowers/specs/2026-06-06-control-plane-critical-review.md`](./docs/superpowers/specs/2026-06-06-control-plane-critical-review.md) — worker heartbeat, Tx isolation contract, catalog MVCC delete/evolve coverage, typed cross-concern identity, and deciding the `Tx` seam's future before any service depends on the library. Deferred features are parked in [`docs/FUTURE.md`](./docs/FUTURE.md).
3. **The services on top — 🚧 underway.** Built so far: the **engine service** (DataFusion against the Iceberg mirror, internal Flight SQL wire) and the **governed query read path** (ontology resolve + ACL compiled into generated SQL, returned as typed-object JSON), now including **governed link traversal** (FK- and join-table-backed links); **Ingest's** load-bearing primitives — the transactional **snapshot-commit** (loom is a native single-catalog Iceberg writer), the **landing materializer** (Arrow → inferred schema → Parquet → snapshot+lineage), and **dataset→model binding** (validated promotion of a landed dataset to an ontology type) — plus the **DataFusion ingestion compute path** (per-call `SessionContext` → repartitioned multi-file Snappy Parquet → per-file Iceberg stats); the runnable **binaries + plain-HTTP endpoints** on the shared `service_runtime`; and an **MVP deploy** (apko/Wolfi OCI images + a Helm chart). Still to come: **Transform workers** (queue-driven DataFusion on `control-plane-worker`), an external SQL wire for third-party clients, distributed/Ballista escalation, and branching.

The slice-by-slice status of record is the documentation registers linked above; this section tracks the headline shape.

## Building & running

```sh
buck2 build //src/...   # build all first-party code — control plane, ingest, query-api (+ hello sample)
buck2 test  //src/...   # run the contract suites (in-memory + hermetic Postgres)
buck2 run   //:<tgt>    # run a target
```

See [`CLAUDE.md`](./CLAUDE.md) for build-system details (cells, bundled prelude, toolchain notes) and [`DEVELOPING.md`](./DEVELOPING.md) for the contributor workflow — getting a checkout building, the dev shell, and day-to-day commands.

## Contributing

High-value tracks right now:

- **Build out the services** — the next slices are the networked ingest/query shells (binaries + endpoints) over the in-process pipeline that already lands, binds, and serves data, plus Transform workers on `control-plane-worker`. The registers call the current front of work.
- **Design pushback** on [`ARCHITECTURE.md`](./ARCHITECTURE.md) — especially the "Open questions" section. Several load-bearing choices haven't been settled; if you see a tradeoff we've gotten wrong, open an issue or a PR against that doc before writing code.
- **Hardening the control-plane library** — the gaps in [`docs/superpowers/specs/2026-06-06-control-plane-critical-review.md`](./docs/superpowers/specs/2026-06-06-control-plane-critical-review.md) (Step 2) are concrete, scoped, and worth landing as the services lean harder on the library.

Each change goes through the same spec → plan → implement → PR cycle the control plane was built with.

## License

TBD.
