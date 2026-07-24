# loom

**An open-source take on Palantir Foundry — a typed-object data platform with built-in lineage and governance: a Rust governance chokepoint over a DataFusion serving layer, with DataFusion for ingestion and transforms, all on an Iceberg + Postgres core.**

> ⚠️ **Status: pre-alpha.** All three service pillars are live. The **control plane** (queue, catalog + Iceberg mirror, ontology, ACL, lineage — plus auth) is built and hardened: five concerns as ports-and-adapters, each with an in-memory fake and a real Postgres adapter behind one backend-agnostic contract. **Ingest** lands Arrow over HTTP through a DataFusion compute path (inferred Iceberg schema → multi-file Parquet → atomic snapshot+lineage commit), binds landed datasets to ontology types, and accepts append-only **log tables** and identity-keyed **CDC tables**. **Transform** runs as queue-driven jobs on a zero-pool worker — physical SQL, typed (ontology-vocabulary), and standing micro-batch queries over stream sources — with inputs streamed over Arrow Flight and the result committed atomically through a single engine RPC. The governed **query API** serves typed-object reads (a typed filter language, link/graph traversal, derived properties, cursor pagination, as-of time travel), governed **actions** (insert/update/delete write-backs with constraints and atomic downstream job enqueue), **vector search** over declared indexes, and two opt-in egress wires (a governed Arrow Flight export and an external Flight SQL wire for third-party clients). An exploratory **web UI** (Yew/WASM), a **Python SDK**, and an MVP **deploy** (apko/Wolfi OCI images, a Helm chart, and an all-in-one `loom` binary with embedded Postgres) ride on top. **Still missing:** distributed DataFusion (Ballista), branching, and plenty of polish — the architecture is still framed as *exploratory*, and it's not a system for general use yet. If you're here to help design or build it, keep reading.

---

## Why loom exists

Palantir Foundry got a few things right that the rest of the data ecosystem mostly hasn't:

- **An ontology, not a table catalog.** Application teams work with `Customer`, `Order`, `Shipment` — typed objects with links between them — instead of joining raw tables.
- **Governance follows the data.** Row- and column-level ACLs, lineage, and audit aren't bolted-on dashboards; they sit in the query path itself.
- **Pipelines, queries, and apps share one substrate.** A transform that produces a dataset and an app that reads it talk about the same objects, with the same access rules.

The catch: Foundry is closed, expensive, and an all-or-nothing commitment. Loom is an attempt to build that *shape* — ontology + lakehouse + governance — from open components, self-hosted, in a stack a small team can actually operate.

## What loom is

Loom is a set of Rust services in front of a DataFusion serving layer and a Postgres database. Reads enter through the **HTTP query API** — the governance chokepoint that resolves the typed ontology to physical tables, applies ACL policy, and generates SQL — which it forwards over **internal Flight SQL** (`CommandStatementQuery`) to the **engine service**. The engine runs **[Apache DataFusion](https://datafusion.apache.org/)** against the Iceberg mirror, streaming Arrow IPC batches back; query-api decodes the batches and returns governed JSON. There is no embedded query engine in query-api itself. **[Apache Iceberg](https://iceberg.apache.org/)** (via `iceberg-rust`, catalog mirrored in Postgres) is the sole table format: data files live as Parquet on S3 or MinIO, and the mirror catalog (`iceberg_mirror.*`) is the Postgres-side MVCC projection the engine reads. DataFusion is also the ingestion and transform engine: bulk writes and queue-driven transform jobs land new Iceberg snapshots. The same Postgres holds the Iceberg mirror, the object/link ontology, ACL policy, the job queue, and lineage events in separate schemas.

For the full design rationale and open questions, see [`ARCHITECTURE.md`](./ARCHITECTURE.md). For what each subsystem can do *today* — behaviour, guarantees, and the key decisions, with PR references — see [`docs/system-capabilities/`](./docs/system-capabilities/README.md).

## Foundry capabilities, mapped to loom

| Foundry concept                  | Loom equivalent                                                                                                  | Status         |
| -------------------------------- | ---------------------------------------------------------------------------------------------------------------- | -------------- |
| **Ontology** (objects, links, properties) | `ontology` schema in Postgres; resolved to physical Iceberg tables at plan time                          | **Built** — resolved to SQL on the read path (objects, properties, derived aggregate properties, links; FK- and join-table-backed traversal, inverse and multi-hop chains, recursive graph queries) |
| **Actions** (typed write-backs)  | Named actions defined alongside object types; governed at the HTTP query API and executed through the engine service, with loom owning the catalog commit so snapshot + lineage + enqueue stay atomic | **Built** — insert/update/delete (single- and multi-step, atomic), model constraints (422s), computed assignments, and `downstream` job templates enqueued atomically with the write |
| **Pipelines / Code Repositories** | Queue-driven transform jobs on a zero-pool worker: inputs stream over Arrow Flight, DataFusion runs the SQL, one engine RPC commits output + lineage atomically | **Built** — physical SQL transforms, typed (ontology-vocabulary, conformance-gated) transforms, and standing micro-batch queries (materialized views) over stream sources |
| **Data Connection** (sources)    | Ingest service — DataFusion writes Parquet in bulk, commits a new Iceberg snapshot                              | **Built** — snapshot commit, landing materializer, dataset→model binding, multi-file DataFusion compute path, plus stream **log** and **CDC** table declaration at land time |
| **Foundry SQL / Contour**        | HTTP query API (governance chokepoint) forwards SQL over internal Flight SQL to the engine service (DataFusion against the Iceberg mirror); returns typed-object JSON | **Built** — governed reads with a typed filter language, object sets, cursor pagination, and as-of time travel; opt-in **external Flight SQL wire** (arbitrary SQL over a governed, closed-world catalog) and **Arrow Flight export** for columnar egress |
| **Markings + project permissions** | `acl` schema — subjects, roles, row- and column-level policy compiled into the SQL the query API emits          | **Built** — deny-by-default, row filters ANDed at every join position, column deny/mask, enforced on reads, traversal, actions, search, and the external wires |
| **Data Lineage**                 | `lineage` schema with [OpenLineage](https://openlineage.io/) events; lineage commits atomically with snapshots   | **Built** — emitted atomically on every write path (landing, flush, transforms, actions, index builds); served via governed `/lineage` reads and rendered as a lineage canvas in the UI |
| **Semantic search / embeddings** | `vector(N)` column type; named per-property indexes (Flat / IVF-Flat / HNSW) built as Puffin sidecars bound to Iceberg snapshots; governed `POST /search` with exact hot-tier merge | **Built** — auto-rebuild on flush keeps indexes fresh; results post-filtered by row policy |
| **Compute backend** (Spark)      | DataFusion (single-node) for serving, ingestion, and transforms; [Ballista](https://datafusion.apache.org/ballista/) optional for distributed transforms | **Serving, ingestion, and transform compute built** (single-node); distributed/Ballista escalation pending |
| **Foundry Branching**            | Iceberg snapshots provide time-travel — `?as_of`/`?as_of_snapshot` reads shipped, GC-retention-aware; named branches TBD | Time travel built; branching open question |

### What's deliberately *not* in scope (for now)

| Foundry capability          | Why loom is skipping it                                                                                          |
| --------------------------- | ---------------------------------------------------------------------------------------------------------------- |
| Workshop / Slate (app UIs)  | App *builders* are a downstream concern. Loom ships one exploratory web UI (login, object explorer, dataset catalog with lineage canvas, transforms admin with a SQL editor) — a reference consumer of the governed API, not an app platform. |
| Notebooks / Code Workbook   | Use Jupyter/Marimo/Hex against the Query API — via the Python SDK, the external Flight SQL wire, or plain HTTP.  |
| AIP (LLM/agent suite)       | Out of scope. Loom is data infrastructure; an LLM layer can sit on top via the Query API and ontology metadata.  |
| Foundry's internal scheduler| Loom uses a Postgres job queue. If you need cron-style scheduling, run an external orchestrator that enqueues jobs. |

The principle: loom is the **data + governance + compute** core. The application layer above it is somebody else's product (or yours).

## How it works (60-second tour)

```
        Consumers / clients (web UI, Python SDK, BI tools, notebooks,
                             Flight SQL wire · Arrow Flight export)
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
        DataFusion — ingestion   ·   Transform workers (queue-driven, zero-pool:
                                     inputs over Arrow Flight, commit via engine RPC)
```

- **One governed front door.** Every read enters the HTTP query API, which resolves the ontology, applies ACL, and compiles a request into SQL — so policy is never bypassable. The opt-in external wires (Flight SQL, Flight export) resolve the same ACL per call; the SQL wire runs client SQL over a closed-world governed catalog rather than rewriting query text.
- **The engine service is the sole serving path.** It runs DataFusion against the Iceberg mirror (`IcebergMirrorTableProvider` for Parquet files, `PgTableProvider` for un-flushed inline rows) and streams Arrow IPC batches back over internal Flight SQL. query-api is a zero-DataFusion wire client — it inlines params, sends SQL, and decodes the result.
- **Postgres is the only stateful coordinator.** The Iceberg mirror catalog, ontology, ACL, queue, and lineage live there in separate schemas, so a single transaction can mutate a snapshot, record lineage, and enqueue downstream work.
- **Iceberg is the sole table format.** Snapshots + Parquet, with a MVCC mirror projection in Postgres so it can share transactions with the control plane. Streams ride the same format: log/CDC tables pair a base table with a changelog, with LastRow compaction and CDC-aware serving reads keeping `GET /objects` correct across the write→flush→consolidate lifecycle.
- **Workers own no state.** Transform workers hold no Postgres pool and no Iceberg catalog — inputs stream over Arrow Flight, compute is DataFusion in the worker, and the commit is a single atomic engine RPC.

For per-component detail, tradeoffs, and the list of decisions still up for debate, read [`ARCHITECTURE.md`](./ARCHITECTURE.md); for the durable record of what's landed, [`docs/system-capabilities/`](./docs/system-capabilities/README.md).

## Tech choices at a glance

- **Language:** Rust for the query API, engine, ingest, and worker services
- **Serving engine:** [Apache DataFusion](https://datafusion.apache.org/) in the engine service — `IcebergMirrorTableProvider` + `PgTableProvider`; internal Flight SQL wire
- **Ingestion / transform compute:** [Apache DataFusion](https://datafusion.apache.org/) (single-node), [Ballista](https://datafusion.apache.org/ballista/) (optional distributed) pending
- **Table format:** [Apache Iceberg](https://iceberg.apache.org/) — via `iceberg-rust` (`iceberg-catalog-sql` backend, Postgres mirror); vector indexes as [Puffin](https://iceberg.apache.org/puffin-spec/) sidecars
- **Query wires:** [Arrow Flight SQL](https://arrow.apache.org/docs/format/FlightSql.html) internally between query-api and the engine; opt-in external governed Flight SQL + Arrow Flight export
- **Control plane:** PostgreSQL (single database, schema-separated: `iceberg_mirror`, `ontology`, `acl`, `queue`, `lineage`, `stream`, `transforms`, `auth`, …)
- **Job queue:** [graphile_worker_rs](https://github.com/leo91000/graphile_worker_rs)–style queue using `SKIP LOCKED` + `LISTEN/NOTIFY`
- **Lineage:** [OpenLineage](https://openlineage.io/) events
- **Object storage:** S3-compatible (S3, MinIO, R2, …)
- **Web UI:** [Yew](https://yew.rs) cross-compiled to WASM (an exploratory surface, built by the same buck2 tree)
- **Python SDK:** `loom-sdk` — sync/async clients over a sans-IO core, optional pydantic ontology-declaration layer
- **Build system:** [Buck2](https://buck2.build)

## Project status & roadmap

Work is tracked as labeled GitHub issues — `roadmap` (committed work), `idea` (deferred ideas), and `bug` (known defects), organized on the `loom v1` project board; the original narrative roadmap is in git history (design doc: `2026-06-06-loom-roadmap`). `main` stays green.

1. **Control-plane library — ✅ delivered.** Five concerns as ports-and-adapters under `src/control-plane/` (`core` traits + domain types, `memory` fake, `postgres` adapter, `testkit` contracts, `worker`): **queue** (with a worker and `await_jobs`), **catalog** (Iceberg mirror read surface), **ontology**, **acl**, and **lineage**. Each runs against one backend-agnostic contract on both the in-memory fake and real Postgres. A cross-concern `Tx` seam makes `emit` + `enqueue` atomic.
2. **Control-plane hardening — ✅ delivered.** Worker heartbeat, Tx isolation contract, catalog MVCC delete/evolve coverage, typed cross-concern identity, compile-time SQL with a committed `.sqlx` cache, pagination, proptest coverage, and a `tracing` pass (design doc in git history: `2026-06-06-control-plane-critical-review`).
3. **The services on top — 🚧 the bulk is built.** The **engine** (DataFusion serving, internal Flight SQL, Iceberg write paths — inline shadow writes, flush, copy-on-write, compaction, GC); the governed **query API** (typed-object reads, filter language, link/graph traversal, derived properties, pagination, time travel, actions, vector search, external egress wires); **ingest** (snapshot commit, landing materializer, dataset→model binding, DataFusion multi-file compute, stream log/CDC declaration); **transform workers** (physical SQL, typed, and standing micro-batch transforms on the zero-pool worker); the **stream engine** (framing/bucketing/offsets, CDC emission, dual base+changelog tables, LastRow compaction, CDC-aware reads); **auth**; the **web UI**; the **Python SDK**; and the **deploy** story (OCI images, Helm chart, all-in-one `loom` binary with embedded Postgres). Still to come: **distributed/Ballista escalation**, **branching**, and the polish tracked issue-by-issue.

The slice-by-slice status of record is the GitHub issue tracker; [`docs/system-capabilities/`](./docs/system-capabilities/README.md) is the durable record of what each subsystem can do today.

## Building & running

```sh
buck2 build //src/...   # build all first-party code — control plane, engine, ingest, query-api, worker, UI, SDK
buck2 test  //src/...   # run the contract suites (in-memory + hermetic Postgres)
buck2 run   //:<tgt>    # run a target
```

See [`CLAUDE.md`](./CLAUDE.md) for build-system details (cells, bundled prelude, toolchain notes) and [`DEVELOPING.md`](./DEVELOPING.md) for the contributor workflow — getting a checkout building, the dev shell, and day-to-day commands. Deployment (images, Helm, the single binary) is documented in [`docs/deploy.md`](./docs/deploy.md).

## Contributing

High-value tracks right now:

- **Pick up ready work** — issues labeled `ready` carry a spec in the body and are claimable; `gh issue list --label ready --no-assignee` shows the current front of work.
- **Design pushback** on [`ARCHITECTURE.md`](./ARCHITECTURE.md) — especially the "Open questions" section. Several load-bearing choices haven't been settled; if you see a tradeoff we've gotten wrong, open an issue or a PR against that doc before writing code.
- **Bugs and hardening** — `bug`-labeled issues are known defects in shipped code; the capability docs' "Known gaps" sections name the sharp edges worth rounding off.

Each change goes through the same spec → plan → implement → PR cycle the control plane was built with.

## License

TBD.
