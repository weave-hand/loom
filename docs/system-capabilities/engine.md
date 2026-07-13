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

_As of 666de0c3._

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
`buck2 cquery` boundary check enforcing the slice-1 invariant (#278). Every
table live in the mirror registers in the serving context: a live-but-empty
table (no inline rows, no data files) serves zero rows under its authoritative
mirror schema (`empty_provider` in `serving.rs`) on every path — `SELECT *`,
previews, governed SQL, and transform inputs alike — while an undeclared or
dropped table, and, for as-of reads, a table not live at the pinned snapshot,
still answers a not-found planning error.

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

**Scalable COW slice 2 — tombstone-aware consolidation** closes the
unbounded-inline-growth gap slice 1 (#331) deliberately left: a shadow-bearing
non-CDC identity table's inline tier (plain appends, `+U` versions, `-D`
tombstones) accrues without bound because the byte-trigger flush is
suppressed for as long as `has_shadow` is set. `consolidate_cow_locked`
(`src/services/engine-serving/src/consolidate.rs`) folds the base Parquet
**and** the entire live inline tier into a new base snapshot — it drains the
plain appends a shadowed table accumulated while its flush was suppressed
too, so consolidation *is* the flush for a mutated table. The fold
materializes `build_merge_view`'s `Precedence::Snapshot` view directly: the
file tier ranks `<0, false>`, the inline tier ranks `<begin_snapshot,
loom_tombstone>`, `ROW_NUMBER() PARTITION BY identity ORDER BY prec DESC`
picks the rank-1 winner per identity, and a tombstoned winner is dropped from
the output — so a read before and after consolidation is byte-identical by
construction.

The commit uses a **consuming** overwrite, not the blanket one:
`overwrite_parquet_snapshot_consuming` (`iceberg_landing.rs`) carries a
targeted `InlineEndCap { table_id, row_ids }` that retires **exactly** the
inline rows the fold actually read, leaving every other live inline row
alone. This closes the race the blanket inline cap left open — a mutation
committing between the fold's read and the overwrite's commit (mutations take
only a per-identity advisory lock, never a table lock) used to be end-capped
without ever being folded, a silent lost write. With the consuming cap that
row survives live and keeps shadowing the new base; the hot mutation path
stays lock-free. `write_mirror`'s dispatch rule now reads: the blanket inline
cap runs only when `overwrite && end_cap.is_none()`, i.e. only for a caller
that has no targeted set to hand it. The same fix applies to CDC compaction's
`consolidate_locked`, which previously blanket-capped mid-consolidation CDC
rows unfolded — an equivalent silent-loss bug, fixed with the same primitive
(closing `iss-consolidate-stream-lost-write`).

`clear_has_shadow_if_quiescent` clears a table's `has_shadow` flag only when
no live shadow delta remains after the consolidating overwrite, re-enabling
the byte-trigger flush for that table. The flush backstop
(`flush_locked`) no longer trusts the flag alone for safety: it derives
correctness from its own read set (`shadow_rows_among`), since a flush can
only corrupt state by appending rows it actually read — a race-free
invariant that holds regardless of the flag's staleness window.

**Dispatch is shared, not duplicated.** `consolidate_stream` now dispatches
through `consolidate_table`, which routes per table kind: a CDC table folds
via the existing `Precedence::Offset` fold, a non-CDC identity table with
`has_shadow` set folds via the new `Precedence::Snapshot` (COW) fold above,
and any other table is a no-op. The `stream_consolidate` job kind and the
`ConsolidateStream` RPC name are unchanged — only the internal dispatch
gained a table-kind branch. Non-CDC mutations now auto-enqueue consolidation
through the same `consolidate_trigger` counter flush/CDC already share (the
`cdc &&` gate that previously excluded them is dropped). Lineage source tags
distinguish the two folds at the same event site: `"consolidate_stream"` for
the CDC path, `"consolidate_cow"` for the COW path. The overwrite commit also
enqueues one deduped `build_vector_index` job per index declared on the
table via `rebuild_jobs_for`, so a vector index's stale cold entries are now
durably removed by consolidation rather than only masked at query time (see
query-api's vector-search post-filter, #400). Slice 3 (identity-change /
upsert) stays deferred as `#fut-cow-identity-change`.

**Small-file compaction auto-triggers at commit** (#419), the file-count analog
of the byte-trigger flush. `#road-compaction-job` shipped compaction as
operator-triggered only (`POST …/compact`); now every **file-adding** commit
counts the table's live sub-cutoff files **in its own commit transaction** and
enqueues a deduped `compact_table` job when the count crosses N. A single
stateless helper `maybe_enqueue_compact` (`iceberg_compact.rs` — one combined
eligibility+count query, then `pg_insert_if_absent`) is called from the three
file-adding tails: `write_mirror` (the CAS path — ingest land, flush, COW
overwrite, stream direct write), `land_additive`, and `IcebergTx::commit` (per
written table). The **loop guard is structural**: compaction's own commit goes
through `register_files(WriteMode::Compact)` / `staged_compacts`, a distinct
path that never reaches the helper, so even a compaction emitting small outputs
cannot re-enqueue from its own commit. Declared stream, changelog, and
shadow-flagged tables are skipped (compacting under live inline deltas could
resurrect tombstoned rows). The enqueue is atomic with the write — the
`pg_notify` is buffered inside `pg_insert_if_absent`'s CTE, so a rolled-back
commit leaks no job — and builds the identical `CompactJob { schema, name }`
payload the operator endpoint uses, so at most one `available` job per table
exists across both producers. Two knobs on both `EngineTuning` and
`RoutingTuning`: `LOOM_COMPACT_THRESHOLD_BYTES` (the small-file cutoff, **shared
with the worker's `small_files` selection** so one deploy value governs counting
and selection) and `LOOM_COMPACT_TRIGGER_FILES` (N — default 8; `0` disables,
`1`/negative rejected at startup, `>= 2` enables). The trigger rides an
`Option<CompactTriggerCfg>` on the `SqlCatalog`, `None` by default, so every
existing caller and fixture is byte-identical unless a service main opts in. The
trigger is stateless — no trigger-state row, no arm/reset protocol; the queue
itself is the debounce. The operator endpoint remains non-deduped and unguarded
(`#iss-compact-endpoint-unguarded`); a stream small-file story
(`#fut-stream-smallfile-compaction`) and a per-table override
(`#fut-compact-trigger-pertable-override`) are deferred.

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
unknown table (#342) — a convenience a caller may still use locally (the
transform worker does, registering the declared schema when its own
serving-path read comes back with zero batches), not a workaround it needs:
internal SQL/wire readers no longer have to self-register empty relations just
to dodge a not-found, since the serving path itself now registers a
live-but-empty table as a zero-row relation (see above). Bulk file data moves
over an **Arrow Flight data plane** on the same socket: a `FlightTicket
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
  **closed-world** — a table absent from the caller's catalog is unresolvable
  to DataFusion, not merely denied, so there is no existence leak through plan
  errors — and runs the client's SQL over them, dispatched by a
  `GovernedStatementQuery` Flight ticket.
- **External SQL wire** (query-api's `FlightSqlWireService`, `flight_sql.rs`):
  a raw TCP `FlightService` making the governed catalog primitive above
  externally consumable, opt-in via `LOOM_SQL_WIRE_BIND_ADDR` (typed
  `SqlWireTuning`, unset by default). Each call authenticates a bearer token
  through the shared `flight_auth` seam (`service_runtime::resolve_bearer` —
  login session tokens and service tokens both accepted), then resolves that
  subject's ACL into a per-request `GovernedCatalog`
  (`resolve_governed_catalog`, deny-by-default) and forwards the client's own
  SQL to the engine's internal governed-SQL plane over the Unix-domain socket
  (`FlightSqlClient::execute_governed_stream` → `GovernedStatementQuery`) —
  the SQL itself is never parsed or rewritten; governance is entirely in the
  catalog the server resolves, not in the query text. `do_get` decodes
  **only** a protobuf `TicketStatementQuery` (the standard Flight SQL
  statement-handle ticket carrying the client's SQL string); it never accepts
  a loom-native ticket such as `GovernedStatementQuery` itself, which would let
  an external caller hand the engine a catalog of its own choosing — a total
  governance bypass. A stream-side row cap (`LOOM_SQL_WIRE_MAX_ROWS`) errors
  the stream once cumulative rows exceed the limit, since the client's
  arbitrary SQL can't be rewritten with a `LIMIT` sentinel the way the export
  path's compiled SQL is. Errors are class-preserving: a plan/validation fault
  from the client's own SQL is `invalid_argument` (safe to echo — it's in the
  client's own vocabulary), everything else is an opaque `internal` (detail
  logged server-side only). Plaintext TCP — TLS is deferred to
  [[fut-flight-export-tls]], behind an operator-supplied terminator.
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

**The MV read-position floor.** Reclaim of a table that is a micro-batch MV
**source** is bounded by a second, non-age condition: the per-bucket
**`mv_floor`** (`control-plane/postgres/src/mv_floor.rs`) — the minimum, across
every MV reading that source, of the MV's committed `stream.mv_watermark`
`next_offset` for the bucket, taking an **absent** watermark row as `0` (an MV's
delta reads an unseen bucket from `0`, so the floor must too, and a
registered-but-never-run MV therefore floors its source at `0`). The reader set
is the union of every registered `MicroBatch`/`MicroBatchJoin` transform def
whose source is the table and every `mv` holding a watermark row against it.
`gc_locked` guards its reclaim with that floor: a **file** is reclaimable only if
its `loom_offset` **max** column stat is strictly below the smallest floor across
all buckets (per-file stats are not per-bucket, so the cross-bucket minimum is
the only sound bound — conservative by construction; a file with **no**
`loom_offset` stat is **held**, the fail-safe direction), and an **inline row**
only if its `(loom_bucket, loom_offset)` is strictly below that bucket's own
floor. A table no MV reads takes a `None` floor and GCs byte-identically to the
pre-floor behavior. Holds are observable — `GcSummary.held_by_mv_floor` counts
the candidates withheld, and a `tracing::warn!` names the per-bucket laggard MV
(the operator's lead to a wedged MV). The **dropped-incarnation** reclaim
deliberately **bypasses** the floor — drop-GC must converge on a table that no
longer exists — and warns, naming the MVs it strands. The escape hatch out of a
dead MV's floor is real: `Transforms::delete_transform` deletes the MV's
watermark rows in the same transaction as the def (certified against both
backends by a testkit contract), so removing a wedged MV's registration releases
its hold.

**What the floor does and does not deliver — read this before relying on it.**
It is **byte-retention defense plus a reusable primitive**, not a guarantee that
an MV cannot lose events. GC only ever reclaims **end-capped** rows/files
(`end_snapshot <= H`), while an MV's delta reads **live** rows at the **current**
snapshot (`engine-serving/src/mv_delta.rs`) — so an end-capped row is already
invisible to the MV before GC touches it, and `gc_table` could never have removed
a row an MV was still able to read. What the guard buys is that a lagging MV's
end-capped **bytes are not destroyed while it is behind** (they stay on disk, the
hold is counted and logged), and that `mv_floor` now exists as the primitive the
paths that *do* create the harm must call. Those are the **end-cap-issuing**
paths — the catalog drop today, and CDC changelog retention, stream small-file
consolidation, and any truncation/replay surface tomorrow: end-capping an offset
an MV has not consumed removes it from the MV's current-snapshot delta
immediately, and no GC-tier guard can bring it back. Making those paths consult
`mv_floor` **before** end-capping is tracked as `#iss-end-cap-ignores-mv-floor`;
two smaller gaps the floor exposed are `#iss-mv-register-below-reclaimed-floor`
(a newly registered MV floors at `0` over a source whose low offsets may already
be gone, plus the floor-read/registration race) and
`#iss-mv-floor-holds-pre-declaration-files` (files written before
`declare_stream` carry no `loom_offset` stat and are held forever by the
fail-safe).

The **third GC source** is an **orphaned-object sweep** — the only path that
reclaims bytes **no mirror row references**, the residue of write-then-commit
crashes (Parquet is written pre-tx) and commit-then-delete degradations (a
failed post-commit object delete). It ships as a warehouse-scoped, schedulable
`sweep_orphans` job (`EngineControl::SweepOrphans`, drained by the zero-pool
worker like `gc_table`): LIST the warehouse, keep only the **pattern-scoped**
data objects (`*.parquet` + `*.puffin` — Iceberg metadata/manifests are excluded
by scope, never diffed), diff them against every mirror-referenced path (**all**
`data_file.path` regardless of `end_snapshot`, plus every
`vector_index.puffin_path`), and delete the unreferenced remainder older than a
write-race grace window (`LOOM_ORPHAN_SWEEP_GRACE_SECS`, default 24h). It never
opens a Postgres transaction around the deletes. Safety is layered: pattern
scoping makes metadata structurally unreachable; the reference set is
over-approximated (every `data_file` row, any `end_snapshot`) so
historical-in-window and dropped-in-window files stay protected; LIST-before-read
ordering plus the grace window make concurrent writers/GC safe; and every
deletion (plus a reference-path that fails to normalize against the warehouse
root) is logged. No dry-run and no HTTP enqueue in v1 — schedules are the
surface; `SweepSummary { objects_deleted, bytes_deleted, candidates_skipped_grace }`
surfaces the counts on the RPC response and in logs.

## Scheduler loop: the engine's first background task

The engine gains its first standing background task alongside the tonic
server: `run::run` spawns `scheduler::scheduler_loop`
(`src/services/engine/src/scheduler.rs`), cancelled when the serve loop exits,
via a dedicated token (a serve-loop error skips the cancel — the process is
exiting either way). Every `LOOM_SCHEDULER_TICK_SECS`
(`EngineTuning::scheduler_tick`, default 5s; `MissedTickBehavior::Delay` so a
slow pass never bursts to catch up), one `tick` calls
`Transforms::claim_due_schedules` against the engine's own `ControlPlane`
handle and submits one `TransformRun` (`trigger: RunTrigger::Schedule`) per
claimed definition through the ordinary `submit_run` path — the loop is a
thin caller over control-plane primitives, with no scheduling logic of its
own. The claim is the concurrency boundary, not the loop: `claim_due_schedules`
runs `SELECT ... FOR UPDATE SKIP LOCKED` and advances each claimed def's
`next_run_at` in the same transaction, so multiple engines ticking against
the same Postgres never double-claim a due definition — the design is
concurrent-engine safe by construction, with no leader election needed. A
claim that fails to make it into a submitted run (a `submit_run` error) is
logged and the occurrence is skipped, not retried, since the clock has
already advanced.

The same loop also fires **maintenance schedules** on the same
`LOOM_SCHEDULER_TICK_SECS` cadence: beside the transform `tick`, a
`maintenance_tick` calls `Queue::fire_due_job_schedules(now, 32)`, which advances
each due `queue.schedule` row and enqueues its `(kind, payload)` maintenance job
(`gc_table` / `compact_table` / `sweep_orphans`) in one transaction — exactly-once, dedup-suppressing
an identical still-`available` job (see [control-plane.md](control-plane.md)). It
mirrors the transform tick's posture — a failed fire is logged and returns zero,
never killing the loop — so one loop, one cadence, no new knob fires both transform
and maintenance schedules.

## Known gaps

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
- `#iss-end-cap-ignores-mv-floor` — the MV read-position floor guards
  `gc_locked` only; the paths that **end-cap** rows an MV has not read (drop
  today; changelog retention / consolidation / truncation tomorrow) do not
  consult `mv_floor`, so the GC-tier guard is byte-retention defense, not
  hole-freedom.
- `#iss-mv-register-below-reclaimed-floor` — a newly registered MV floors at
  offset `0` over a source whose low offsets may already be reclaimed, and the
  floor read sits outside the GC transaction (registration race).
- `#iss-mv-floor-holds-pre-declaration-files` — files written before
  `declare_stream` carry no `loom_offset` stat, so the fail-safe guard holds
  them forever and inflates `held_by_mv_floor`.
