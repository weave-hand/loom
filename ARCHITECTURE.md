# Architecture

> **Status:** exploratory. This document describes one candidate direction for loom, not a committed design. Treat the choices below as defaults to argue with — when something here turns into a firm decision, move it out of "Open questions" and into the body, and update this banner once the overall shape stabilizes.

## What loom is

Loom is an internal **lakehouse + ontology platform**. The lakehouse part stores tabular data as Parquet on object storage with a transactional catalog; the ontology part puts a typed object/link model on top so application teams can work with domain entities (Customer, Shipment, …) instead of raw tables. ACLs, lineage, and a job queue all live next to the catalog so policy and provenance follow the data rather than being bolted on later.

The intended users are internal data and application teams — people who want Foundry-shaped ergonomics (typed objects, governed access, lineage) without buying Foundry.

## High-level shape

```
            Clients (BI tools, notebooks, app code)
                            |
                       Quack / HTTP
                            |
        +-------------------+-------------------+
        v                   v                   v
   Ingest service    Transform workers     Query API
   (Rust+DF,         (Rust+DF, queue,      (Rust+DF, ontology+ACL,
    Quack server)     Quack server)         Quack server)
         \                  |                    /
          \           [Ballista pool]           /
           \           (optional scale-out)   /
            \                |               /
             v               v              v
        +----------------------------------------------+
        |       Postgres — unified control plane       |
        |  ducklake.*  ontology   queue                |
        |  lineage     acl                             |
        +----------------------------------------------+
                            |
                            v
                Object storage (S3 / MinIO)
                Parquet files, DuckLake-managed layout
```

Three Rust services share one Postgres instance and one object store. **Postgres is the only stateful coordinator** — catalog metadata, the job queue, lineage events, the ontology, and ACL policy all live in it, in separate schemas. Parquet on S3/MinIO holds the actual data; catalog rows in Postgres reference object paths.

## Why this shape

Four choices are doing most of the work here:

1. **DuckLake as the table format.** DuckLake keeps the *catalog* in Postgres (rather than in a separate metastore or in object storage as JSON manifests like Iceberg). That makes it natural to put everything else that needs transactional consistency with the catalog — ontology, ACL, queue, lineage — in the same database, in their own schemas. One transaction can mutate a snapshot *and* update lineage *and* enqueue downstream work.

2. **DataFusion as the shared compute engine.** All three services embed DataFusion, so query rewriting (e.g. ACL row/column filters, ontology → physical-table resolution) is implemented once against a single logical-plan API. **Ballista** is held in reserve for transforms that outgrow a single node; it's the same engine, just distributed.

3. **Postgres as the job queue.** A custom Rust queue using `SELECT … FOR UPDATE SKIP LOCKED` for fairness and `LISTEN/NOTIFY` for low-latency wakeups — the same pattern as [`graphile_worker_rs`](https://github.com/leo91000/graphile_worker_rs), which we either adopt directly or use as the reference implementation. Avoids running a second broker (Kafka, NATS, SQS) for what is, at this scale, a tractable workload — and lets transform jobs be enqueued transactionally with the catalog mutation that produced them.

4. **[Quack](https://duckdb.org/docs/current/quack/overview) as the wire protocol.** Every Rust service — Ingest, Transform, Query API — exposes a Quack endpoint. External clients (BI tools, notebooks, application code) get a DuckDB-compatible client experience: `ATTACH` a loom service and remote tables behave like local ones. Internally, services that need to talk to each other (e.g. a Transform worker pulling intermediate results from a peer) use the same protocol instead of a bespoke RPC. One wire format, one set of client libraries, one auth surface. Note Quack runs *over* DataFusion here — our services translate inbound Quack queries into DataFusion plans, run them, and serialize results back in DuckDB's internal format. That translation layer is real engineering work, and Quack itself is still beta — see "Related work" for caveats.

## Components

### Services

| Service               | Role                                                                                                                                  |
| --------------------- | ------------------------------------------------------------------------------------------------------------------------------------- |
| **Ingest service**    | Accepts incoming data, writes Parquet to object storage, commits a new DuckLake snapshot.                                             |
| **Transform workers** | Long-running workers that pull jobs from `queue`, run DataFusion plans, write new snapshots. Optionally dispatch to a Ballista pool.  |
| **Query API**         | Serves reads over a Quack endpoint. Resolves ontology references to physical tables, applies ACL row/column policy, plans with DataFusion.            |

All three are Rust binaries built against the same crates for: DuckLake client, ontology resolver, ACL enforcement, lineage emission, and the Quack-over-DataFusion server shim.

### Postgres schemas

| Schema       | Contents                                                                                                                                              |
| ------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------- |
| `ducklake.*` | DuckLake's own tables — snapshots, file references, per-column stats, schemas. Owned by the DuckLake client; loom code reads but should not write directly. |
| `ontology`   | Typed objects, links between types, properties, and named actions. The mapping from ontology types to physical DuckLake tables also lives here.       |
| `queue`      | Custom queue (graphile-worker-style). Job rows with status, attempt count, payload; dequeue via `SKIP LOCKED`, wake workers via `NOTIFY`.             |
| `lineage`    | OpenLineage events, run history, DAG edges between datasets and runs. Append-mostly; readers reconstruct provenance from events.                      |
| `acl`        | Subjects (users, service accounts), roles, and row/column policy bound to ontology types or physical tables.                                          |

Schemas are isolated so each can evolve independently, but they live in one database so we get cross-schema transactions when we need them.

### Object storage

S3 (or MinIO in dev) holds Parquet files in the layout DuckLake's writer expects. The catalog is the source of truth for which files belong to which snapshot — orphaned files left behind by aborted writes are reclaimed by a separate GC pass (TBD; see open questions).

## Cross-cutting concerns

### Ontology
The ontology layer is the user-facing model: types like `Customer` and `Order`, links like `Order.customer → Customer`, properties, and named actions. The Query API resolves ontology references to underlying DuckLake tables at plan time; transforms can read and write through the ontology too, so lineage is recorded at the type level rather than the file level.

### ACL
Row- and column-level policy is enforced by rewriting DataFusion logical plans before execution: ACL predicates are pushed into scans, restricted columns are projected out. The Query API enforces; transforms generally run as a privileged subject but can be scoped down for user-initiated jobs.

### Lineage
Every snapshot-producing operation (ingest, transform) emits OpenLineage events into the `lineage` schema. Because lineage shares a database with the catalog and the queue, the emitted event, the snapshot commit, and any downstream job enqueue can be one atomic transaction — no "lineage drift" between what happened and what was recorded.

## Related work & building blocks

These projects aren't dependencies *yet*, but they shape the design and are the obvious places to crib from (or depend on outright):

- **[`graphile_worker_rs`](https://github.com/leo91000/graphile_worker_rs)** — Rust port of the `graphile-worker` Postgres job queue. Implements exactly the `SKIP LOCKED` + `LISTEN/NOTIFY` pattern the `queue` schema needs, with attempt counts, retries, and crashed-worker recovery already worked out. Default plan is to model the queue on this; potentially depend on it directly if its schema is acceptable.
- **[Quack](https://duckdb.org/docs/current/quack/overview)** — DuckDB's remote-access extension: HTTP/HTTPS, default port 9494, one request/response per query after the handshake, results serialized in DuckDB's internal format (complex types preserved losslessly). Supports both stateless `quack_query()` calls and full `ATTACH`, so a Quack-fronted service can be mounted as a remote catalog inside a client DuckDB. This is the wire protocol all three loom services expose; we depend on it as a building block, not just as inspiration. **Caveat:** Quack is explicitly beta — "the protocol, function names, settings, and defaults are still subject to change." We need to pin a known-good DuckDB/Quack version and treat protocol upgrades as breaking changes until it stabilizes.

## Open questions

These are the design choices that aren't settled. If you're working in this repo and resolve one, please move it from this list into the body of the doc.

- **Multi-writer ingest.** Is there one ingest service, or can multiple ingest writers commit concurrently? DuckLake's concurrency model needs to be matched to whatever ingest topology we pick.
- **ACL pushdown completeness.** Some predicates (e.g. policies that depend on joining against another ontology type) can't be pushed into a single scan. What's the fallback — plan-level filter, or refuse to plan?
- **Ontology authoring & migration.** Who edits types/links, and how do schema changes propagate to existing physical tables and snapshots? Manual migration tool, or generated?
- **Ballista trigger heuristics.** When does a transform escalate from single-node DataFusion to the Ballista pool — input size? explicit annotation? cost estimate?
- **GC of orphaned Parquet.** Crashed writers leave dangling object-store files. Periodic sweep against the catalog? Compaction job?
- **Queue durability vs latency.** `LISTEN/NOTIFY` is best-effort — workers also need a polling fallback for missed notifications. What's the polling interval, and do we need a separate "stuck job" reaper?
- **Tenancy.** Single-tenant per deployment, or multiple logical tenants behind one control plane? Affects how aggressively `acl` and `ontology` need to be partitioned.
- **Quack-over-DataFusion translation surface.** How much of Quack's expected behavior do we implement? Stateless `quack_query()` is straightforward; full `ATTACH` semantics (transactions, prepared statements, DuckDB-specific types/functions clients may use) are a larger surface. What's the minimum viable subset, and what do we return when a client uses a feature we don't support?
- **Quack version pinning.** Quack is beta; protocol/defaults can change. Which DuckDB release do we pin to, and what's the upgrade story when DuckDB ships a breaking Quack change?
