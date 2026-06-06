# loom

[![CI](https://github.com/weave-hand/loom/actions/workflows/ci.yml/badge.svg)](https://github.com/weave-hand/loom/actions/workflows/ci.yml)

**An open-source take on Palantir Foundry — a typed-object data platform with built-in lineage and governance, running on a Rust + DataFusion + DuckLake core.**

> ⚠️ **Status: pre-alpha.** The **control-plane library is built** — five concerns (queue, catalog, ontology, ACL, lineage) as ports-and-adapters, each with an in-memory fake and a real Postgres adapter, run against a shared backend-agnostic contract. The three **services** that consume it (Ingest, Transform, Query API) and the Quack wire shim are **not built yet**. The architecture is still framed as *exploratory*: the shape is sketched, several load-bearing decisions are flagged for hardening. If you're here to use loom, the answer is "not yet." If you're here to help design or build it, keep reading.

---

## Why loom exists

Palantir Foundry got a few things right that the rest of the data ecosystem mostly hasn't:

- **An ontology, not a table catalog.** Application teams work with `Customer`, `Order`, `Shipment` — typed objects with links between them — instead of joining raw tables.
- **Governance follows the data.** Row- and column-level ACLs, lineage, and audit aren't bolted-on dashboards; they sit in the query path itself.
- **Pipelines, queries, and apps share one substrate.** A transform that produces a dataset and an app that reads it talk about the same objects, with the same access rules.

The catch: Foundry is closed, expensive, and an all-or-nothing commitment. Loom is an attempt to build that *shape* — ontology + lakehouse + governance — from open components, self-hosted, in a stack a small team can actually operate.

## What loom is

Loom is three Rust services and a Postgres database. The services (Ingest, Transform workers, Query API) all embed [Apache DataFusion](https://datafusion.apache.org/) as their compute engine. They read and write tabular data through [DuckLake](https://ducklake.select/) — an open lakehouse format whose catalog lives *in Postgres* rather than in a separate metastore. The same Postgres also holds the typed object/link ontology, ACL policy, the job queue, and lineage events, in separate schemas. Data files live as Parquet on S3 or MinIO. External clients talk to loom via [Quack](https://duckdb.org/docs/current/quack/overview), DuckDB's remote protocol — so any DuckDB client can `ATTACH` a loom service and use it like a remote catalog, even though DataFusion is doing the work underneath.

For the full design rationale and open questions, see [`ARCHITECTURE.md`](./ARCHITECTURE.md).

## Foundry capabilities, mapped to loom

| Foundry concept                  | Loom equivalent                                                                                                  | Status         |
| -------------------------------- | ---------------------------------------------------------------------------------------------------------------- | -------------- |
| **Ontology** (objects, links, properties) | `ontology` schema in Postgres; resolved to physical DuckLake tables at plan time                          | Library built; resolution-at-plan-time pending services |
| **Actions** (typed write-backs)  | Named actions defined alongside object types; executed as transactional ontology + catalog mutations             | Designed, not built |
| **Pipelines / Code Repositories** | Transform workers pulling jobs from the `queue` schema; DataFusion plans against DuckLake snapshots             | Queue + worker library built; transform service not built |
| **Data Connection** (sources)    | Ingest service — accepts incoming data, writes Parquet, commits a new DuckLake snapshot                          | Designed, not built |
| **Foundry SQL / Contour**        | Query API exposing a Quack endpoint; clients use any DuckDB-compatible SQL surface                               | Designed, not built |
| **Markings + project permissions** | `acl` schema — subjects, roles, row- and column-level policy pushed into DataFusion plans                      | Library built; plan pushdown pending Query API |
| **Data Lineage**                 | `lineage` schema with [OpenLineage](https://openlineage.io/) events; lineage commits atomically with snapshots   | Library built (one-hop, `emit`+`enqueue` atomic); transitive + catalog leg pending |
| **Compute backend** (Spark)      | DataFusion single-node by default; optional [Ballista](https://datafusion.apache.org/ballista/) for scale-out    | Designed, not built |
| **Foundry Branching**            | DuckLake snapshots provide time-travel; named branches TBD                                                        | Open question  |

### What's deliberately *not* in scope (for now)

| Foundry capability          | Why loom is skipping it                                                                                          |
| --------------------------- | ---------------------------------------------------------------------------------------------------------------- |
| Workshop / Slate (app UIs)  | UI builders are a downstream concern. Loom exposes Quack; build whatever UI you like on top.                     |
| Notebooks / Code Workbook   | Use Jupyter/Marimo/Hex against the Query API.                                                                    |
| AIP (LLM/agent suite)       | Out of scope. Loom is data infrastructure; an LLM layer can sit on top via the Query API and ontology metadata.  |
| Foundry's internal scheduler| Loom uses a Postgres job queue. If you need cron-style scheduling, run an external orchestrator that enqueues jobs. |

The principle: loom is the **data + governance + compute** core. The application layer above it is somebody else's product (or yours).

## How it works (60-second tour)

```
        Clients (BI tools, notebooks, app code)
                       │
                  Quack / HTTP
                       │
       ┌───────────────┼───────────────┐
       │               │               │
  Ingest service  Transform workers  Query API
       │               │               │
       └───────────────┼───────────────┘
                       │
        Postgres (DuckLake catalog,
        ontology, ACL, queue, lineage)
                       │
              S3 / MinIO  (Parquet)
```

- **Postgres is the only stateful coordinator.** Catalog, ontology, ACL, queue, and lineage all live there in separate schemas, so a single transaction can mutate a snapshot, record lineage, and enqueue downstream work.
- **DataFusion is the compute engine everywhere.** ACL rewriting and ontology resolution are implemented once, against a single logical-plan API.
- **Quack is the wire protocol.** External clients and inter-service calls speak the same protocol — one set of client libs, one auth surface.
- **DuckLake is the table format.** Snapshots + Parquet, with the catalog co-located in Postgres so it can share transactions with everything else.

For per-component detail, tradeoffs, and the list of decisions still up for debate, read [`ARCHITECTURE.md`](./ARCHITECTURE.md).

## Tech choices at a glance

- **Language:** Rust across all services
- **Compute:** [Apache DataFusion](https://datafusion.apache.org/) (single-node), [Ballista](https://datafusion.apache.org/ballista/) (optional distributed)
- **Table format:** [DuckLake](https://ducklake.select/), via [`datafusion-ducklake`](https://github.com/hotdata-dev/datafusion-ducklake)
- **Wire protocol:** [Quack](https://duckdb.org/docs/current/quack/overview) — DuckDB-compatible remote access
- **Control plane:** PostgreSQL (single database, schema-separated)
- **Job queue:** [graphile_worker_rs](https://github.com/leo91000/graphile_worker_rs)–style queue using `SKIP LOCKED` + `LISTEN/NOTIFY`
- **Lineage:** [OpenLineage](https://openlineage.io/) events
- **Object storage:** S3-compatible (S3, MinIO, R2, …)
- **Build system:** [Buck2](https://buck2.build)

## Project status & roadmap

Three steps, tracked in [`docs/superpowers/specs/2026-06-06-loom-roadmap.md`](./docs/superpowers/specs/2026-06-06-loom-roadmap.md). `main` stays green.

1. **Control-plane library — ✅ delivered.** Five concerns as ports-and-adapters under `src/control-plane/` (`core` traits + domain types, `memory` fake, `postgres` adapter, `testkit` contracts, `worker`): **queue** (with a worker and `await_jobs`), **catalog** (DuckLake read surface), **ontology**, **acl**, and **lineage**. Each runs against one backend-agnostic contract on both the in-memory fake and real Postgres. A cross-concern `Tx` seam makes `emit` + `enqueue` atomic.
2. **Harden the control plane.** Correctness and contract gaps catalogued in [`docs/superpowers/specs/2026-06-06-control-plane-critical-review.md`](./docs/superpowers/specs/2026-06-06-control-plane-critical-review.md) — worker heartbeat, Tx isolation contract, catalog MVCC delete/evolve coverage, typed cross-concern identity, and deciding the `Tx` seam's future before any service depends on the library. Deferred features are parked in [`docs/FUTURE.md`](./docs/FUTURE.md).
3. **The services on top.** Quack-over-DataFusion shim, then Query API (ontology resolve + ACL pushdown), Ingest (Parquet writes, snapshot commits, lineage), and Transform workers (built on `control-plane-worker`). Optional Ballista escalation, ontology actions, branching beyond that.

When something gets built, this section moves it from "planned" into a concrete pointer.

## Building & running

```sh
buck2 build //src/...   # build the control-plane crates (+ the hello_world sample)
buck2 test  //src/...   # run the contract suites (in-memory + hermetic Postgres)
buck2 run   //:<tgt>    # run a target
```

See [`CLAUDE.md`](./CLAUDE.md) for build-system details (cells, bundled prelude, toolchain notes) and [`DEVELOPING.md`](./DEVELOPING.md) for the contributor workflow — getting a checkout building, the dev shell, and day-to-day commands.

## Contributing

Two high-value tracks right now:

- **Design pushback** on [`ARCHITECTURE.md`](./ARCHITECTURE.md) — especially the "Open questions" section. Several load-bearing choices haven't been settled; if you see a tradeoff we've gotten wrong, open an issue or a PR against that doc before writing code.
- **Hardening the control-plane library** — the gaps in [`docs/superpowers/specs/2026-06-06-control-plane-critical-review.md`](./docs/superpowers/specs/2026-06-06-control-plane-critical-review.md) (Step 2) are concrete, scoped, and need to land before the services consume the library.

Each change goes through the same spec → plan → implement → PR cycle the control plane was built with.

## License

TBD.
