# Architecture

> **Status:** exploratory. This document describes one candidate direction for loom, not a committed design. Treat the choices below as defaults to argue with — when something here turns into a firm decision, move it out of "Open questions" and into the body, and update this banner once the overall shape stabilizes.

## What loom is

Loom is an internal **lakehouse + ontology platform**. The lakehouse part stores tabular data as Parquet on object storage with a transactional catalog; the ontology part puts a typed object/link model on top so application teams can work with domain entities (Customer, Shipment, …) instead of raw tables. ACLs, lineage, and a job queue all live next to the catalog so policy and provenance follow the data rather than being bolted on later.

The intended users are internal data and application teams — people who want Foundry-shaped ergonomics (typed objects, governed access, lineage) without buying Foundry.

## High-level shape

```
                 Consumers / clients (BI tools, notebooks, app code)
                                     |
                                 HTTP / Quack
                                     |
                     +-------------------------------+
                     |  HTTP query API — chokepoint  |   ontology + governance (Rust)
                     |  resolves ontology refs to SQL|
                     +-------------------------------+
                                     |  resolves to SQL
                                     v
                     +-------------------------------+
                     |     DuckDB — serving layer    |   ATTACH ducklake · speaks Quack
                     |   reads + governed action-    |   (real DuckDB, native DuckLake)
                     |   writes                      |
                     +-------------------------------+
                                     |  reads / action-writes
                                     v
        +------------------------------------------------------------+
        |                     DuckLake — storage                     |
        |  Catalog: Postgres (ducklake.* + ontology, acl, queue,     |
        |  lineage)              Data files: Parquet on S3 / MinIO    |
        +------------------------------------------------------------+
                  ^                                       ^
                  | writes (bulk)                         | new snapshots
       +---------------------------+        +-----------------------------+
       |  DataFusion — ingestion   |        |     Transform workers       |
       |  custom operators,        |        |  queue-driven, DataFusion / |
       |  complex compute          |        |  optional Ballista          |
       +---------------------------+        +-----------------------------+
```

Reads flow through one front door: clients hit the **HTTP query API**, the Rust chokepoint that resolves ontology references and applies governance, generates SQL, and hands it to a **DuckDB serving layer**. That serving layer is *real DuckDB* — it `ATTACH`es the DuckLake catalog, reads the Parquet data files, and speaks [Quack](https://duckdb.org/docs/current/quack/overview) natively, so loom doesn't reimplement the wire protocol. **Writes split by shape:** bulk ingestion runs on **DataFusion** (custom operators, heavy compute) and lands new snapshots directly; queue-driven **Transform workers** likewise write snapshots with DataFusion (optionally Ballista); and low-volume, governed **actions** route *through* the serving layer — entering at the HTTP chokepoint and executing against DuckLake.

**Postgres is the only stateful coordinator.** It is simultaneously the DuckLake catalog (`ducklake.*`) and the loom control plane — the job queue, lineage events, the ontology, and ACL policy live in it as separate schemas. Parquet on S3/MinIO holds the actual data; catalog rows in Postgres reference object paths.

## Why this shape

Five choices are doing most of the work here:

1. **DuckLake as the table format.** DuckLake keeps the *catalog* in Postgres (rather than in a separate metastore or in object storage as JSON manifests like Iceberg). That makes it natural to put everything else that needs transactional consistency with the catalog — ontology, ACL, queue, lineage — in the same database, in their own schemas. One transaction can mutate a snapshot *and* update lineage *and* enqueue downstream work.

2. **DuckDB as the serving engine.** Reads — and governed action-writes — are served by *real DuckDB*, which is already a native DuckLake reader/writer and already speaks Quack. Putting it on the read path means loom does not reimplement a query engine *or* the Quack wire protocol for serving; it `ATTACH`es the catalog and runs the SQL it's handed. The Rust **HTTP query API** sits in front as the governance chokepoint: it owns ontology resolution and ACL, turns a request into SQL (with policy predicates and projections baked in), and forwards that SQL to DuckDB. Every read passes this front door, so governance is never bypassable.

3. **DataFusion as the ingestion & transform engine.** DataFusion is *not* on the read path. It powers bulk ingestion (custom operators, complex compute, high-throughput Parquet writes) and the queue-driven Transform workers, where a programmable logical-plan API earns its keep. **Ballista** is held in reserve for transforms that outgrow a single node; it's the same engine, just distributed.

4. **Postgres as the job queue.** A custom Rust queue using `SELECT … FOR UPDATE SKIP LOCKED` for fairness and `LISTEN/NOTIFY` for low-latency wakeups — the same pattern as [`graphile_worker_rs`](https://github.com/leo91000/graphile_worker_rs), which we either adopt directly or use as the reference implementation. Avoids running a second broker (Kafka, NATS, SQS) for what is, at this scale, a tractable workload — and lets transform jobs be enqueued transactionally with the catalog mutation that produced them.

5. **[Quack](https://duckdb.org/docs/current/quack/overview) as the wire protocol.** External clients (BI tools, notebooks, application code) get a DuckDB-compatible client experience: `ATTACH` loom's serving layer and remote tables behave like local ones. Because the serving layer *is* DuckDB, Quack is spoken natively rather than emulated — no translation shim, no protocol gaps. One wire format, one set of client libraries. Quack itself is still beta, so we pin a known-good DuckDB version and treat protocol upgrades as breaking changes — see "Related work" for caveats.

## Components

### Services

| Component               | Role                                                                                                                                                              |
| ----------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **HTTP query API**      | The chokepoint and only public front door (Rust). Resolves ontology references to physical tables, applies ACL row/column policy, and generates the SQL it forwards to the serving layer. Owns the transaction for governed action-writes. |
| **DuckDB serving layer**| Real DuckDB processes that `ATTACH` the DuckLake catalog, execute the SQL handed to them (reads and governed action-writes), and speak Quack to clients. No loom query-engine code runs here. |
| **Ingest service**      | Bulk path: accepts incoming data, runs DataFusion (custom operators, heavy compute), writes Parquet to object storage, commits a new DuckLake snapshot.            |
| **Transform workers**   | Long-running Rust workers that pull jobs from `queue`, run DataFusion plans, write new snapshots. Optionally dispatch to a Ballista pool.                          |

The Rust pieces (HTTP query API, Ingest, Transform workers) are built against the same crates for: DuckLake catalog access, ontology resolution, ACL enforcement, lineage emission, and the transactional snapshot-commit primitive. The serving layer is unmodified DuckDB — loom configures and fronts it rather than embedding a custom engine there.

### Postgres schemas

| Schema       | Contents                                                                                                                                              |
| ------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------- |
| `ducklake.*` | DuckLake's own tables — snapshots, file references, per-column stats, schemas. Written by whoever commits a snapshot: DuckDB at the serving layer, and loom's own Rust snapshot-commit primitive on the ingest/action path (so a commit can share one transaction with lineage and the queue). The single-catalog row layout is kept byte-compatible with DuckDB's. |
| `ontology`   | Typed objects, links between types, properties, and named actions. The mapping from ontology types to physical DuckLake tables also lives here.       |
| `queue`      | Custom queue (graphile-worker-style). Job rows with status, attempt count, payload; dequeue via `SKIP LOCKED`, wake workers via `NOTIFY`.             |
| `lineage`    | OpenLineage events, run history, DAG edges between datasets and runs. Append-mostly; readers reconstruct provenance from events.                      |
| `acl`        | Subjects (users, service accounts), roles, and row/column policy bound to ontology types or physical tables.                                          |

Schemas are isolated so each can evolve independently, but they live in one database so we get cross-schema transactions when we need them.

### Object storage

S3 (or MinIO in dev) holds Parquet files in the layout DuckLake's writer expects. The catalog is the source of truth for which files belong to which snapshot — orphaned files left behind by aborted writes are reclaimed by a separate GC pass (TBD; see open questions).

## Cross-cutting concerns

### Ontology
The ontology layer is the user-facing model: types like `Customer` and `Order`, links like `Order.customer → Customer`, properties, and named actions. The HTTP query API resolves ontology references to underlying DuckLake tables when it generates SQL; transforms can read and write through the ontology too, so lineage is recorded at the type level rather than the file level.

### ACL
Row- and column-level policy is enforced where the SQL is generated: the HTTP query API bakes ACL predicates into `WHERE` clauses and projects restricted columns out before the statement ever reaches the serving layer, so DuckDB only sees a query the subject is allowed to run. (This replaces the earlier plan-rewrite approach — with DuckDB serving, there is no loom-controlled logical plan on the read path to rewrite.) Transforms generally run as a privileged subject but can be scoped down for user-initiated jobs.

### Lineage
Every snapshot-producing operation (ingest, transform, action) emits OpenLineage events into the `lineage` schema. Because lineage shares a database with the catalog and the queue, the emitted event, the snapshot commit, and any downstream job enqueue can be one atomic transaction — no "lineage drift" between what happened and what was recorded. For action-writes, loom keeps ownership of the catalog-commit transaction (see *Actions*); lineage can be injected at write time or deferred to compaction.

### Actions
Actions are the ontology's typed write-backs — named mutations like "approve order" defined alongside object types. They enter through the HTTP query API (which authorizes the subject, resolves the ontology references, and generates SQL) and are executed against DuckLake *through the serving layer*, sharing the one engine and the one governed front door with reads. Crucially, loom retains ownership of the catalog-commit transaction via the Rust snapshot-commit primitive, so an action's snapshot, its lineage event, and any downstream job enqueue remain one atomic unit. The exact point lineage is attached — at write time vs. folded in at compaction — is left open (see open questions); it does not change the transactional shape.

## Related work & building blocks

These projects aren't dependencies *yet*, but they shape the design and are the obvious places to crib from (or depend on outright):

- **[`graphile_worker_rs`](https://github.com/leo91000/graphile_worker_rs)** — Rust port of the `graphile-worker` Postgres job queue. Implements exactly the `SKIP LOCKED` + `LISTEN/NOTIFY` pattern the `queue` schema needs, with attempt counts, retries, and crashed-worker recovery already worked out. Default plan is to model the queue on this; potentially depend on it directly if its schema is acceptable.
- **[Quack](https://duckdb.org/docs/current/quack/overview)** — DuckDB's remote-access extension: HTTP/HTTPS, default port 9494, one request/response per query after the handshake, results serialized in DuckDB's internal format (complex types preserved losslessly). Supports both stateless `quack_query()` calls and full `ATTACH`, so a Quack-fronted service can be mounted as a remote catalog inside a client DuckDB. Because loom's serving layer *is* DuckDB, Quack is spoken natively — loom depends on it as a building block rather than reimplementing it. **Caveat:** Quack is explicitly beta — "the protocol, function names, settings, and defaults are still subject to change." We pin a known-good DuckDB/Quack version and treat protocol upgrades as breaking changes until it stabilizes.

## Open questions

These are the design choices that aren't settled. If you're working in this repo and resolve one, please move it from this list into the body of the doc.

- **Multi-writer ingest.** Three things can commit snapshots now — the DuckDB serving layer (actions), the DataFusion ingest path (bulk), and Transform workers — possibly concurrently. DuckLake's concurrency model and loom's advisory-lock serialization need to be matched to that topology, and bulk writes that bypass the serving layer must stay consistent with what DuckDB later reads.
- **Serving-layer write transactionality.** Actions execute *through* DuckDB, but loom wants the snapshot + lineage + enqueue to commit as one Postgres transaction (the snapshot-commit primitive). DuckDB owns its own connection/transaction, so how does loom wrap or hand off the catalog commit — generate the DuckLake mutation but commit it on loom's connection, inject lineage at write time, or reconcile at compaction? This is the load-bearing detail behind the *Actions* design.
- **ACL pushdown completeness.** Policy is now compiled into the SQL the query API emits. Some predicates (e.g. policies that depend on joining against another ontology type) can't be expressed as a simple `WHERE`/projection rewrite. What's the fallback — a generated subquery/CTE, or refuse to serve the request?
- **Ontology authoring & migration.** Who edits types/links, and how do schema changes propagate to existing physical tables and snapshots? Manual migration tool, or generated?
- **Ballista trigger heuristics.** When does a transform escalate from single-node DataFusion to the Ballista pool — input size? explicit annotation? cost estimate?
- **GC of orphaned Parquet.** Crashed writers leave dangling object-store files. Periodic sweep against the catalog? Compaction job?
- **Queue durability vs latency.** `LISTEN/NOTIFY` is best-effort — workers also need a polling fallback for missed notifications. What's the polling interval, and do we need a separate "stuck job" reaper?
- **Tenancy.** Single-tenant per deployment, or multiple logical tenants behind one control plane? Affects how aggressively `acl` and `ontology` need to be partitioned.
- **Serving-layer topology & isolation.** How many DuckDB serving processes, how are they shared across tenants/subjects, and how is per-request identity carried from the HTTP API into a DuckDB connection without leaking state between requests?
- **Quack version pinning.** Quack is beta; protocol/defaults can change. Which DuckDB release do we pin the serving layer to, and what's the upgrade story when DuckDB ships a breaking Quack change?
