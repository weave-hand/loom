# Engine capabilities

This document describes what the engine subsystem can do today: the engine
service (the sole process that owns Postgres, the Iceberg catalog, and the
object store), the internal Flight SQL wire that query-api consumes as a
zero-DataFusion client, DataFusion serving over the loom-owned Iceberg mirror,
the Iceberg write paths (append commit, inline shadow writes, flush,
overwrite/copy-on-write, transform registration), per-column stats and
predicate pushdown, physical GC, and the engine-wire control plane that
zero-pool workers and query-api drive over a Unix-domain socket. It is
distilled from the closed register items assigned to the subsystem and their
design specs; open work is listed at the end.

_As of 4861433b._

## Serving path and the internal Flight SQL wire

The engine service is loom's **sole serving path**. DataFusion query execution
lives in the `engine-serving` crate and is hosted by the engine binary; DuckDB
and the DuckLake table format were removed outright, leaving Iceberg (served
by DataFusion inside the engine process) as the only way loom writes and reads
tables (#199). query-api is a **zero-DataFusion wire client**: it compiles
governed, param-inlined SQL and sends it to the engine over the engine's
Unix-domain socket, decoding the Arrow IPC result. The execution tier
(`IcebergMirrorTableProvider`, `PgTableProvider`, file pruning) was relocated
out of query-api into `engine-serving` (#181), and the original unary
`ExecuteQuery` RPC — one buffered IPC blob per result, subject to the ~4MB
gRPC message cap — was replaced by **streamed reads over internal Flight SQL**
(`CommandStatementQuery` → `get_flight_info` → `do_get`, encoded via
`FlightDataEncoderBuilder` from `df.execute_stream()`, so the engine never
holds a whole result) (#189). The whole tree is on a single arrow major (58),
achieved by sourcing iceberg-rust from a pinned `main` commit — the tree's
only git dependency — which deleted the arrow-57 target families and every
cross-major batch conversion (#184).

Serving robustness on this path: the recursive-CTE `/graph` reads were proven
on DataFusion — the graph compilers had to alias the anchor CTE columns
explicitly (`0 AS depth`) because DataFusion ignores the CTE column-name list
(#82, #118); backend/serving faults that surface as an opaque 500 are now
logged server-side with full detail via the shared tracing subscriber
(`service_runtime::init_tracing`) while the client body stays opaque (#140);
and query-api's library is guarded postgres-free — a regression that re-added
a direct `control-plane/postgres` dependency was caught and reverted, with a
`buck2 cquery` boundary check enforcing the slice-1 invariant (#278).

## Iceberg catalog: pointer + mirror

loom vendors and owns the Iceberg SQL catalog (ported from
`iceberg-catalog-sql` to sqlx 0.9), giving a three-layer design: the canonical
Iceberg metadata in object store (spec-compliant, written via iceberg-rust);
a JDBC-layout **pointer** table in Postgres; and a loom-owned
**`iceberg_mirror.*` projection** — snapshots, live file lists, columns,
per-column stats — MVCC-versioned by `begin_snapshot`/`end_snapshot`, from
which all loom reads are served without ever walking object-store manifests
(#76). `IcebergCatalog` implements the `core::Catalog` trait over the mirror
and passes the same backend-agnostic contract suite the previous backend did.
Because loom owns the catalog's `update_table`, the pointer compare-and-swap
and the mirror projection commit in **one Postgres transaction** — a reader
never sees the pointer ahead of the mirror; the mirror remains a rebuildable
projection of canonical state as defense-in-depth.

## Write paths: append commit, transaction scoping, CAS retry

The real-Parquet write path drives Arrow batches through the iceberg writer
chain and commits a `fast_append`, with snapshot ids allocated from a Postgres
sequence (concurrency-safe, monotonic) (#78). The commit transaction body is
**pure local Postgres**: the sole in-transaction object-store read (manifest
enumeration for the mirror projection) was hoisted before `begin()`, and the
in-tx writer (`write_mirror`) takes precomputed inputs with no `FileIO` — a
type-level guarantee that no object-store I/O happens while the transaction
(and its locks) are held (#78, #138). Lineage and inline end-caps ride the
same transaction via a `CommitExtras` decorator, so a snapshot, its lineage
event, and any inline-row retirement land or roll back together.

Multi-writer contention is handled by a bounded, backed-off **CAS-conflict
retry** in the `append_batches*` family: on a lost pointer CAS the writer
reloads the table (fresh parent snapshot), re-stages the same already-written
data files (UUID-prefixed, written once), and re-commits — exponential backoff
with jitter, ~5 attempts, then the original retryable error propagates (#191).
An N=8 concurrent-append contention test pins the invariant (exactly N
snapshots, zero orphans, all N files).

**Overwrite/replace** exists as a mirror-faithful primitive
(`overwrite_parquet_snapshot`): end-cap every live `data_file` row at the new
snapshot, project the new files (with their stats), Iceberg append, and
lineage with delete/insert change segments — all in one transaction, with
prior snapshots still time-travelling to the replaced files (#152). Truncation
(overwrite with zero files) is valid. Both overwrite shapes enqueue a deduped
`build_vector_index` rebuild per index declared on the table, sharing the flush
path's `rebuild_jobs_for` seam so k-NN never silently serves a pre-overwrite
index (#338). This primitive is the basis of loom's
governed UPDATE/DELETE **copy-on-write**: the `OverwriteTable` engine RPC
performs UPDATE/DELETE via whole-table copy-on-write, and `WriteObject`
performs the governed typed-insert — both relocated from query-api into
engine-serving so query-api's `ActionEngine` is a thin pre-authorized wire
client and the row+lineage transaction stays atomic inside the engine (#234).
Whole-table COW is no longer the only mutation shape: scalable COW slice 1
(#331) lands an identity-targeted UPDATE/DELETE as an **inline-shadow delta**
(O(change), not O(table)) guarded by a per-identity CAS, with the serving path
building a merge-on-read view — latest shadow version wins per identity — over
the base Parquet; identity-less types keep the whole-table path.
Governed typed-inserts route through the inline-write seam, so an action row
and its lineage commit in one Postgres transaction and are immediately
readable through the engine (#151).

**Transform output** commits through a genuinely polymorphic `Tx`: an
`IcebergControlPlane`/`IcebergTx` stages `create_table`/`append_files`/
`replace_files`/`emit` and applies them — registering transform's
already-written `DataFile`s, Iceberg pointer-CAS, mirror projection, lineage —
atomically in one Postgres transaction at `commit()` (#165).

**Additive schema evolution** replaced the silent project-once mirror
divergence: a land whose schema appends nullable columns at the end projects
only the new columns (MVCC-stamped at the evolving snapshot, with the
snapshot's `schema_version` bumped); any other change — drop, rename, retype,
reorder, non-nullable add — is rejected with a typed error that aborts the
whole commit (#179). The serving path is mirror-authoritative (no
Parquet-footer schema inference) and null-fills the new column for
pre-evolution files. The inline path gets detect+reject parity.

Wire-data hygiene: the inline decode's `dc!` downcast macro is fallible — a
decoded IPC batch whose arrow type mismatches the declared logical column is
rejected as a validation error instead of panicking the write path (#296).

## Inline shadow writes and the flush vertical

Small writes avoid the tiny-Parquet problem by landing as typed rows in
per-table `iceberg_mirror.inline_<table_id>` Postgres tables — no object-store
file, no Iceberg metadata change — committed with their lineage in one
transaction, and **unioned with the table's Parquet files at read time** by
the serving engine (#86). The ingest landing backend routes each request by
in-memory byte size (`LOOM_INLINE_BYTE_LIMIT`): small requests inline, large
ones write real Parquet (#90). Inline rows are served **directly from
Postgres** through a purpose-built DataFusion `PgTableProvider` (projection/
filter/limit pushdown into the inline `SELECT`, MVCC base predicate baked in
per query) rather than being re-encoded to in-memory Parquet on every read
(#90, #181).

The accepted tradeoff — inline rows are invisible to external Iceberg clients
until flushed — is bounded by the **flush vertical** (#86, #96, #103, #108):

- `flush_table` (#96) drains a table's live inline rows into a real Iceberg
  Parquet snapshot and end-caps them at the same snapshot, atomically —
  exactly-once and time-travel-correct by the shared MVCC predicate (the file
  is live at `at >= S`, the inline rows at `at < S`), serialized per table by
  an advisory lock.
- A byte-size **trigger** on `inline_append` (#103) accrues live inline bytes
  in `iceberg_mirror.inline_trigger` and transactionally enqueues one
  debounced `flush_table` job when `LOOM_FLUSH_BYTE_THRESHOLD` is crossed;
  every flush call — including no-ops — leaves the trigger disarmed.
- A **zero-pool consumer worker** (#108) drains flush jobs over the engine
  wire: the engine owns Postgres and serves the tonic `EngineControl` service
  on a UDS; the worker's dependency graph structurally excludes
  `control-plane-postgres`.

Flush is idempotent under at-least-once redelivery: two concurrent
`flush_table` dispatches for one table produce exactly-once effect (the
advisory-locked loser re-reads and no-ops), pinned by an over-the-wire
regression test (#134).

## Engine-wire control plane and the Arrow Flight data plane

The engine exposes a typed gRPC `EngineControl` surface on its UDS —
queue ops (`Dequeue`/`Complete`/`Fail`/`Heartbeat`/`AwaitJobs`, the last a
NOTIFY-bridged long-poll) plus maintenance and write RPCs (`FlushTable`,
`CompactTable`, `GcTable`, `WriteObject`, `OverwriteTable`, and
`CommitTransform` — the transform commit, mirroring `CompactTable`'s
conventions, which stages create+append/replace+lineage in one engine-side
transaction (#342)) — consumed by zero-pool workers via `GrpcQueueClient`
(#108). `ListFiles` responses carry the table's declared schema as
`columns_json` (absent ⟺ the table does not exist), so a wire consumer can
register a zero-file table as an empty relation and distinguish it from an
unknown table (#342). Bulk file data moves over an
**Arrow Flight data plane** on the same socket: a `FlightTicket
{schema, name, files}` streams a named file set as schema-first Arrow IPC, so
a compaction worker streams exactly the small files it will coalesce, rewrites
them, and commits the swap back over the wire — the engine mediates all table
data and does no compaction compute (#157). The file-ticket `do_get` enforces
**live-snapshot membership**: every ticket-named path is cross-checked against
the table's current file set before any bytes are read, rejecting unknown
paths without echoing them (#157, #244).

Governance metadata also crosses the wire: query-api reads ACL policy and
ontology over typed per-read `EngineControl` RPCs carrying serde-encoded
`core` domain payloads (`WireAcl`/`WireOntology` composed into a read-only
`WireControlPlane`), making the engine the sole reader of the governance
schema; query-api's remaining Postgres use is auth resolution and the GC
enqueue (#261).

## Governed SQL over arbitrary queries, and external egress

Two governed egress surfaces exist beyond the internal read path:

- **Governed catalog primitive** (#274): the correctness core for running
  **arbitrary** SQL under ACL by putting governance in the relations, not a
  SQL rewriter. A `GovernedTableProvider` decorates the mirror provider with a
  per-type `(row_filters, denied, masked)` policy and is enforcing by
  construction — `schema()` drops denied columns and re-types masked ones,
  `scan()` unconditionally ANDs the row filter on the full schema (so filters
  may reference denied columns) before projection and `'***'` masking.
  `execute_governed_sql_stream` registers one governed provider per live table
  and runs the client's SQL over them, dispatched by a `GovernedStatementQuery`
  Flight ticket. Internal-only today; the external TCP listener is the
  deferred next slice.
- **Governed Arrow Flight export** (#204): query-api hosts a TCP Flight
  server whose ticket carries a loom export *command* (typed object + slice
  filters), never SQL. `do_get` re-derives the ACL'd SQL per call for the
  bearer-token-authenticated subject (no `LIMIT`, capped by
  `LOOM_EXPORT_MAX_ROWS`) and streams the engine's batches straight out —
  `vector(N)` columns carried natively as `List<Float32>`, bypassing the
  JSON row-flattening.

## Stats and predicate pushdown

The write path records per-file, per-column min/max, null counts, and sizes in
`iceberg_mirror.data_file_column_stat`, computed from the Parquet footer in
the same transaction as the snapshot commit, for both the append and flush
paths (#132). `IcebergCatalog::files_with_stats` surfaces them, and the
serving `IcebergMirrorTableProvider` evaluates a `PruningPredicate` per file
to **skip whole Parquet files** that provably cannot match the query's full
`WHERE` (which includes the compiled ACL row filters) — files with no stats
are always kept, so pruning can never change a governed result, only avoid
I/O. Overwritten/replaced files carry stats immediately; a backfill for
pre-feature files was considered and dropped as unnecessary.

## Object storage and path resolution

The Iceberg backend runs against S3-compatible object storage: the
`StorageFactory` and warehouse URI are env-driven and scheme-selected
(`LOOM_WAREHOUSE_URI`; `file://` → local, `s3://` → S3 with standard `AWS_*`
credentials, path-style implied for MinIO), proven by a hermetic MinIO
fixture round-trip that lands and reads a table with `s3://` mirror paths
(#182). Paths are opaque URLs end to end. Two path defects on transform
chains were fixed: `write_dataset` output is absolutized at write time via
`absolute_data_files` so relative paths can never reach the mirror, and
`scan_table` was made scheme-aware — an absolute `FileRef` path used
verbatim with its derived object store registered, sharing one
`object_store_url_for` resolver with the serving engine (#245). With
transforms on the engine wire, `scan_table` no longer sits on a production
path — the wire path registers Flight-fetched batches via `register_batches`
— so it survives as datafusion-io's tested read helper, while
`object_store_url_for` remains the resolver the serving engine shares.

## GC

Physical reclamation is an operator-triggered, per-table `gc_table(schema,
name)` under an **age-based retention horizon** (`LOOM_GC_RETENTION_SECS`):
rows with `end_snapshot <= H` — provably invisible to every in-window
time-travel read — are deleted along with their Parquet via a `FileIO`
`delete_file` seam, **commit-then-delete** so a failed object delete degrades
to a deferred orphan and never a dangling mirror reference, serialized under
the flush advisory lock (#194). The engine-wire shape matches flush: a
maintenance endpoint enqueues a `gc_table` job, a zero-pool worker drains it
via `EngineControl::GcTable`, and the engine executes. Slice 2 extends
`gc_table` to **dropped tables**: dropped incarnations of a name (across
drop/recreate history) have their data files reclaimed under the same
horizon, and once fully reclaimed the physical `inline_<tid>` table and the
`table`/`column` mirror rows are removed — gated on full reclaim so nothing
time-travellable vanishes (#266).

## Known gaps

- `#fut-external-sql-wire` — the external SQL wire (TCP listener + auth over
  the governed-catalog engine path).
- `#fut-flight-sql-surface` — the rest of the Flight SQL command surface
  (prepared statements, catalog-metadata commands).
- `#fut-flight-export-tls` — TLS/mTLS on the external Flight export wire.
- `#fut-flight-export-config-seam` — fold the Flight export knobs into the
  typed config seam.
- `#fut-serving-stream-to-http` — stream reads through to the HTTP client
  instead of collecting `Rows`.
- `#fut-iceberg-manifest-bounds` — decode Iceberg-manifest bounds instead of
  re-reading Parquet footers.
- `#fut-iceberg-footer-write-time-stats` — footer-only / at-write-time stats
  computation.
- `#fut-iceberg-pruning-cost-estimates` — pruning-aware cost estimates and
  join ordering.
- `#fut-iceberg-full-schema-evolution` — field_id persistence; rename/drop/
  retype/reorder evolution.
- `#fut-iceberg-additive-inline` — additive schema evolution on the inline
  path (detect+reject only today).
- `#fut-iceberg-time-travel-schema` — as-of-schema reconstruction for
  time-travel reads.
- `#fut-iceberg-schema-cache` — schema cache for the serving engine's
  per-query table registration.
- `#fut-iceberg-stats-dedup` — unify the iceberg/datafusion parquet-footer
  stats readers.
- `#fut-iceberg-gc-orphan-sweep` — object-store-listing sweep for orphaned
  Parquet (write-then-commit failures, failed GC deletes).
- `#fut-iceberg-retry-cap-tunable` — env-tunable CAS-commit retry cap.
- `#fut-iceberg-s3-multipart` — S3 multipart upload for large data files.
- `#fut-iceberg-s3-serving-e2e` — end-to-end serving read against S3.
- `#fut-awaitjobs-stream` — persistent-stream `AwaitJobs` instead of the
  unary long-poll.
- `#fut-engine-wire-multi-tls` — multiple engines, pooling, TLS/auth on the
  engine socket (currently one UDS, local trust).
- `#fut-auth-wire-resolve` — auth resolution over the engine wire (toward the
  credential-free query-api binary).
- `#fut-queue-wire-enqueue` — GC enqueue over the engine wire.
- `#fut-wire-governance-cache` — cache wire-fetched governance reads.
- `#fut-iceberg-external-oracle` — an independent external Iceberg reader as
  a cross-check oracle.
