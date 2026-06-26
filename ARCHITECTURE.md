# Architecture

> **Status:** exploratory, but no longer all on paper. The control plane and the first ingest/query slices are built, and several load-bearing choices below have landed in code: **Iceberg is the sole table format** (vendored `iceberg-catalog-sql` + an `iceberg_mirror.*` MVCC projection), the **engine service is the sole serving path** (DataFusion against the Iceberg mirror, served over internal Flight SQL), and loom's **native single-catalog snapshot-commit primitive** exists (catalog + lineage + enqueue in one Postgres transaction). The remaining choices are still defaults to argue with. For live, slice-by-slice status see [`README.md`](./README.md) and the roadmap spec it links; when something here turns into a firm decision, move it out of "Open questions" and into the body.

## What loom is

Loom is an internal **lakehouse + ontology platform**. The lakehouse part stores tabular data as Parquet on object storage with a transactional Iceberg catalog; the ontology part puts a typed object/link model on top so application teams can work with domain entities (Customer, Shipment, …) instead of raw tables. ACLs, lineage, and a job queue all live next to the catalog so policy and provenance follow the data rather than being bolted on later.

The intended users are internal data and application teams — people who want Foundry-shaped ergonomics (typed objects, governed access, lineage) without buying Foundry.

## High-level shape

```
             Consumers / clients (BI tools, notebooks, app code)
                                  |
                              HTTP / REST
                                  |
                  +-------------------------------+
                  |  HTTP query API — chokepoint  |   ontology + governance (Rust)
                  |  resolves ontology refs to SQL|   zero-DataFusion wire client
                  +-------------------------------+
                                  |  Flight SQL (internal, UDS)
                                  v
                  +-------------------------------+
                  |   Engine — DataFusion serving |   IcebergMirrorTableProvider
                  |   execute_query_stream over   |   + PgTableProvider (inline)
                  |   CommandStatementQuery        |
                  +-------------------------------+
                                  |  reads Parquet + inline rows
                                  v
     +------------------------------------------------------------+
     |                  Iceberg — storage                         |
     |  Mirror catalog: Postgres (iceberg_mirror.* + ontology,    |
     |  acl, queue, lineage)   Data files: Parquet on S3 / MinIO  |
     +------------------------------------------------------------+
               ^                                       ^
               | writes (bulk)                         | new snapshots
    +---------------------------+        +-----------------------------+
    |  DataFusion — ingestion   |        |     Transform workers       |
    |  custom operators,        |        |  queue-driven, DataFusion / |
    |  complex compute          |        |  optional Ballista          |
    +---------------------------+        +-----------------------------+
```

Reads flow through one front door: clients hit the **HTTP query API**, the Rust chokepoint that resolves ontology references and applies governance, generates SQL, and forwards it over **internal Flight SQL** (`CommandStatementQuery`) to the **engine service**. The engine runs DataFusion against the Iceberg mirror — `IcebergMirrorTableProvider` for Parquet files (with per-column stats + predicate pushdown) and `PgTableProvider` for un-flushed inline rows — and streams Arrow IPC batches back. query-api decodes the batches and returns governed JSON. **Writes split by shape:** bulk ingestion runs on **DataFusion** (custom operators, heavy compute) and lands new Iceberg snapshots directly; queue-driven **Transform workers** likewise write snapshots with DataFusion (optionally Ballista); and low-volume, governed **actions** route through `iceberg_landing::land` (the inline-write seam), committing the row and its lineage in one Postgres transaction.

**Postgres is the only stateful coordinator.** It is simultaneously the Iceberg mirror catalog (`iceberg_mirror.*`) and the loom control plane — the job queue, lineage events, the ontology, and ACL policy live in it as separate schemas. Parquet on S3/MinIO holds the actual data; catalog rows in Postgres reference object paths.

## Why this shape

Five choices are doing most of the work here:

1. **Iceberg as the table format.** Iceberg is the industry-standard open lakehouse format; loom's mirror (`iceberg_mirror.*`) keeps a Postgres-side MVCC projection of the Iceberg metadata so that every concern — ontology, ACL, queue, lineage — can share one database. One transaction can mutate a snapshot *and* update lineage *and* enqueue downstream work. The real Iceberg metadata (manifest lists + manifests) lives on object storage as usual; the mirror is the fast, transactional index loom reads for serving. *(Built: full Iceberg read + write path via `iceberg-catalog-sql`, inline writes, flush + compaction, overwrite/replace mode, per-column stats + predicate pushdown, additive schema evolution, CAS-conflict retry, real S3/MinIO backend, and physical GC.)*

2. **DataFusion-in-the-engine as the serving engine; query-api as a Flight SQL wire client.** Reads are served by the **engine service**, which runs DataFusion (`IcebergMirrorTableProvider` + `PgTableProvider`) and exposes an internal Flight SQL surface (`CommandStatementQuery`) on a Unix domain socket. query-api is a **zero-DataFusion wire client**: it inlines params, sends SQL to the engine over Flight SQL, and decodes the streamed Arrow IPC batches — no embedded query engine in query-api itself. Every read still passes through the HTTP query API governance chokepoint (ontology resolution + ACL compilation into generated SQL), so policy is never bypassable. *(Built: the engine binary hosts `EngineQueryService` on the same UDS as `EngineControl` and Arrow Flight; query-api's `EngineServingClient` dials the UDS via `engine_wire::client`; streaming is via `execute_query_stream` → Arrow IPC → `batches_to_rows`. Proven by a 600k-row e2e that would have exceeded the old unary gRPC cap.)*

3. **DataFusion as the ingestion & transform engine.** DataFusion is *not* on the read path. It powers bulk ingestion (custom operators, complex compute, high-throughput Parquet writes) and the queue-driven Transform workers, where a programmable logical-plan API earns its keep. **Ballista** is held in reserve for transforms that outgrow a single node; it's the same engine, just distributed. *(Built: the ingest **write path runs on DataFusion** — a per-call `SessionContext` size-estimates and repartitions the input into N Snappy Parquet files, then commits per-file Iceberg stats with the snapshot+lineage transaction. Ingest ships as a runnable **binary** with an **HTTP landing endpoint** (`POST /datasets/...`, Arrow IPC). Transform workers and distributed Ballista remain future.)*

4. **Postgres as the job queue.** A custom Rust queue using `SELECT … FOR UPDATE SKIP LOCKED` for fairness and `LISTEN/NOTIFY` for low-latency wakeups — the same pattern as [`graphile_worker_rs`](https://github.com/leo91000/graphile_worker_rs), which we either adopt directly or use as the reference implementation. Avoids running a second broker (Kafka, NATS, SQS) for what is, at this scale, a tractable workload — and lets transform jobs be enqueued transactionally with the catalog mutation that produced them.

5. **Internal Flight SQL as the engine wire; external wire deferred.** The engine's internal Flight SQL surface (`CommandStatementQuery`) is the data plane between query-api and the engine. Exposing it externally (adding a TCP listener, TLS, and SQL-governance) is deliberately deferred — the internal-first half is built. A Quack-*client* `ServingEngine` seam exists for a possible future Quack topology, but loom-as-a-Quack-*server* is not built. *(Built: `FlightService` on the engine UDS dispatches `TicketStatementQuery` to the SQL path and the legacy JSON `FlightTicket` to the file path. `engine_wire::FlightSqlClient` provides the zero-pool client. Proven by a streaming-equivalence test, an engine-level Flight SQL wire test, and a query-api e2e.)*

## Components

### Services

| Component               | Role                                                                                                                                                              |
| ----------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **HTTP query API**      | The chokepoint and only public front door (Rust). Resolves ontology references to physical tables, applies ACL row/column policy, generates SQL, and forwards it to the engine over internal Flight SQL. Owns the transaction for governed action-writes. |
| **Engine service**      | DataFusion serving tier: hosts `EngineQueryService` (Flight SQL), `EngineControl` (flush/compact/GC RPCs), and Arrow Flight file-read on a Unix domain socket. Runs `IcebergMirrorTableProvider` + `PgTableProvider` for reads. Owns Postgres and the S3/MinIO object store. |
| **Ingest service**      | Bulk path: accepts incoming data, runs DataFusion (custom operators, heavy compute), writes Parquet to object storage, commits a new Iceberg snapshot.            |
| **Transform workers**   | Long-running Rust workers that pull jobs from `queue`, run DataFusion plans, write new snapshots. Optionally dispatch to a Ballista pool.                          |

The Rust pieces (HTTP query API, Ingest, Engine, Transform workers) are built against the same crates for: Iceberg catalog access, ontology resolution, ACL enforcement, lineage emission, and the transactional snapshot-commit primitive. *(Built: the **Ingest service** and **HTTP query API** ship as runnable **binaries** with **HTTP endpoints** — ingest with a DataFusion write path, query-api with governed reads + link traversal — plus the **Engine service** (DataFusion serving + flight), and a deploy MVP (apko/Wolfi OCI images + Helm chart). **Transform workers** and ontology actions remain future.)*

### Postgres schemas

| Schema             | Contents                                                                                                                                              |
| ------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------- |
| `iceberg_mirror.*` | loom's MVCC projection of Iceberg metadata — snapshots, data file references, per-column stats, table/column definitions. Written by whoever commits a snapshot: loom's own Rust snapshot-commit primitive (append, overwrite, compact, flush, GC). The single-catalog row layout mirrors Iceberg's own metadata for time-travel correctness. |
| `ontology`         | Typed objects, links between types, properties, and named actions. The mapping from ontology types to physical Iceberg tables also lives here.       |
| `queue`            | Custom queue (graphile-worker-style). Job rows with status, attempt count, payload; dequeue via `SKIP LOCKED`, wake workers via `NOTIFY`.             |
| `lineage`          | OpenLineage events, run history, DAG edges between datasets and runs. Append-mostly; readers reconstruct provenance from events.                      |
| `acl`              | Subjects (users, service accounts), roles, and row/column policy bound to ontology types or physical tables.                                          |

Schemas are isolated so each can evolve independently, but they live in one database so we get cross-schema transactions when we need them.

### Object storage

S3 (or MinIO in dev) holds Parquet files in the layout Iceberg's writer expects. The mirror catalog is the source of truth for which files belong to which snapshot — orphaned files left behind by aborted writes are reclaimed by a separate GC pass (age-based per-table `gc_table`; orphaned-Parquet listing sweep is future).

## Cross-cutting concerns

### Ontology
The ontology layer is the user-facing model: types like `Customer` and `Order`, links like `Order.customer → Customer`, properties, and named actions. The HTTP query API resolves ontology references to underlying Iceberg tables when it generates SQL; transforms can read and write through the ontology too, so lineage is recorded at the type level rather than the file level.

### ACL
Row- and column-level policy is enforced where the SQL is generated: the HTTP query API bakes ACL predicates into `WHERE` clauses and projects restricted columns out before the statement ever reaches the engine, so DataFusion only sees a query the subject is allowed to run. Transforms generally run as a privileged subject but can be scoped down for user-initiated jobs.

### Lineage
Every snapshot-producing operation (ingest, transform, action) emits OpenLineage events into the `lineage` schema. Because lineage shares a database with the catalog and the queue, the emitted event, the snapshot commit, and any downstream job enqueue can be one atomic transaction — no "lineage drift" between what happened and what was recorded.

### Actions
Actions are the ontology's typed write-backs — named mutations like "approve order" defined alongside object types. They enter through the HTTP query API (which authorizes the subject, resolves the ontology references, and generates the insert) and are executed via `iceberg_landing::land` (the inline-write seam), committing the row and its `LineageEvent` in one Postgres transaction. The exact point lineage is attached — at write time vs. folded in at compaction — is left open (see open questions); it does not change the transactional shape.

## Related work & building blocks

These projects aren't dependencies *yet*, but they shape the design and are the obvious places to crib from (or depend on outright):

- **[`graphile_worker_rs`](https://github.com/leo91000/graphile_worker_rs)** — Rust port of the `graphile-worker` Postgres job queue. Implements exactly the `SKIP LOCKED` + `LISTEN/NOTIFY` pattern the `queue` schema needs, with attempt counts, retries, and crashed-worker recovery already worked out. Default plan is to model the queue on this; potentially depend on it directly if its schema is acceptable.
- **[`iceberg-rust`](https://github.com/apache/iceberg-rust)** — Apache Iceberg Rust implementation. loom vendors `iceberg-catalog-sql` (sqlx 0.9 backend) and sources iceberg from a pinned `main` commit (git dep), carrying the arrow/parquet 58 migration ahead of the post-0.9.1 crates.io release.
- **[Arrow Flight SQL](https://arrow.apache.org/docs/format/FlightSql.html)** — The internal query wire between query-api and the engine (`CommandStatementQuery`). The `do_get` dispatching, `FlightDataEncoderBuilder` streaming, and `FlightSqlClient` are the live building blocks for an eventual external SQL wire.

## Open questions

These are the design choices that aren't settled. If you're working in this repo and resolve one, please move it from this list into the body of the doc.

- **Multi-writer ingest.** Three things can commit snapshots now — the DataFusion ingest path (bulk), the inline-write seam (actions), and Transform workers — possibly concurrently. Iceberg's optimistic CAS + the mirror's retry/backoff handle the common case; the edge cases under heavy contention and the advisory-lock serialization scope need to be matched to the full topology.
- **Action write transactionality.** The snapshot-commit primitive is now built and proven on the **ingest** and **action** paths (loom natively writes Iceberg snapshots + lineage in one Postgres transaction). What's still open is the actions path for *governed action-writes that must also trigger downstream enqueue* — the `emit` + `enqueue` seam is built, but a governed write that uses it needs the `Tx` to be held through the action's DataFusion plan if ever actions grow beyond simple inserts.
- **ACL pushdown completeness.** Policy is now compiled into the SQL the query API emits. Some predicates (e.g. policies that depend on joining against another ontology type) can't be expressed as a simple `WHERE`/projection rewrite. What's the fallback — a generated subquery/CTE, or refuse to serve the request?
- **Ontology authoring & migration.** Who edits types/links, and how do schema changes propagate to existing physical tables and snapshots? Manual migration tool, or generated?
- **Ballista trigger heuristics.** When does a transform escalate from single-node DataFusion to the Ballista pool — input size? explicit annotation? cost estimate?
- **GC of orphaned Parquet.** Crashed writers leave dangling object-store files. Age-based GC (`gc_table`) reclaims mirror-tracked end-capped rows; an orphaned-Parquet listing sweep (files on object storage with no mirror reference) is still future.
- **Queue durability vs latency.** `LISTEN/NOTIFY` is best-effort — workers also need a polling fallback for missed notifications. What's the polling interval, and do we need a separate "stuck job" reaper?
- **Tenancy.** Single-tenant per deployment, or multiple logical tenants behind one control plane? Affects how aggressively `acl` and `ontology` need to be partitioned.
- **External SQL wire.** The engine's internal Flight SQL surface is built (UDS, `CommandStatementQuery`). Exposing it externally (TCP, TLS, auth, SQL-governance for arbitrary queries) is the deferred next step — see `fut-external-sql-wire`.
