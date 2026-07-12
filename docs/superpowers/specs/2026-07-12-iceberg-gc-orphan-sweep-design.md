# Orphaned-Parquet GC sweep Design

> **Status:** design (direction). This spec makes `road-iceberg-gc-orphan-sweep`
> build-ready (promoted 2026-07-12 from `#fut-iceberg-gc-orphan-sweep`). A
> separate work agent writes the implementation plan from it and builds it.

## Problem

loom's two GC slices reclaim only **mirror-referenced** dead bytes: `gc_table`
(`postgres/src/iceberg_gc.rs:68-183`) deletes end-capped `data_file` rows past
the retention horizon and the objects they reference, and the dropped-table
pass reclaims a dropped incarnation wholesale. Neither can touch a file **no
mirror row references**. Such orphans accrue from two designed-in degradation
paths:

- **Write-then-commit failures.** Every landing path writes Parquet as "pure
  IO, pre-tx" before any Postgres transaction opens (`iceberg_landing.rs:462`,
  `:540-541`); the compaction worker likewise writes coalesced output before
  the commit RPC (`datafusion-io/src/write.rs:164-169`). A crash between PUT
  and commit strands the objects forever.
- **Commit-then-delete degradations.** `gc_table` itself commits the mirror
  delete first and deletes objects after; a failed object delete is logged and
  left behind (`iceberg_gc.rs:163-176`) — the module header explicitly names
  this residue "the already-deferred orphaned-Parquet class"
  (`iceberg_gc.rs:30-34`).

Nothing ever lists the warehouse and asks "what does no row reference?". This
is the third and final GC source.

## Decision record

**Operator decisions, 2026-07-12 (committed):**

- Ships as a **schedulable job kind** (`sweep_orphans`), an immediate consumer
  of the `/admin/schedules` surface landed by
  `#road-scheduled-maintenance-jobs` (PR #422).
- **No dry-run mode in v1.** Safety comes from strict pattern scoping, the
  write-race grace window, and per-deletion logging (see Safety posture).
- Grace window is a global env knob (`LOOM_ORPHAN_SWEEP_GRACE_SECS`, default
  24h), mirroring `LOOM_GC_RETENTION_SECS`.

## Context — what ships today (verified)

- **Job/schedule plumbing to mirror.** `GC_JOB_KIND`/`GcJob`
  (`core/src/gc.rs:6-13`); `SCHEDULABLE_JOB_KINDS = &[GC_JOB_KIND,
  COMPACT_JOB_KIND]` (`core/src/job_schedule.rs:15`) with a per-kind payload
  decode arm in `validate_job_schedule` (`job_schedule.rs:46-62`);
  `KNOWN_JOB_KINDS` (`core/src/queue.rs:32-41`); worker dispatch
  (`worker/src/main.rs:91-128`, `handle_gc` at `handler.rs:45-54` — one RPC,
  parse error → Abandon, RPC error → Retry); engine execution
  (`engine/src/service.rs:162-179` → `iceberg_gc::gc_table`). The schedule
  firing path already dedups via `pg_insert_if_absent`
  (`postgres/src/queue.rs:258`).
- **`schedule_table_check`** (`runtime/src/admin.rs:1636-1664`) validates the
  `{schema,name}` payload for `gc_table`/`compact_table` only; any other kind
  falls through untouched (`admin.rs:1641-1643`) — a warehouse-scoped kind
  needs no change there.
- **Object-store LIST exists.** `store.list(Some(&prefix))` streaming
  `ObjectMeta` is already used by `delete_prefix`
  (`iceberg_sql_catalog/s3_storage.rs:201-210`) and the coalesce writer's
  discovery pass (`datafusion-io/src/write.rs:171-198`). `ObjectMeta` carries
  `location`, `size`, and **`last_modified`** — the grace signal.
- **The store handle.** `ObjectStoreConfig::parse`
  (`store-config/src/lib.rs:94-149`) selects local/S3 by `LOOM_WAREHOUSE_URI`
  scheme; `build_write_store` (`lib.rs:215-236`) returns
  `WriteStore { store: Arc<dyn ObjectStore>, root_url }` — listable +
  deletable, plus the absolute root for URI normalization. The engine already
  holds warehouse config (`engine/src/run.rs`).
- **Where file references live** (the full reference set):
  1. `iceberg_mirror.data_file.path` (`migrations/0012_iceberg_mirror.sql:46-55`)
     — **all rows regardless of `end_snapshot`**: live files AND end-capped
     files still inside the retention window (in-window time-travel reads
     them; only `gc_table` may age them out).
  2. `iceberg_mirror.vector_index.puffin_path` (`0018_vector_index.sql:12`) —
     Puffin sidecars.
  3. `iceberg_tables.metadata_location`/`previous_metadata_location`
     (`iceberg_sql_catalog/catalog.rs:45-51,302`) — Iceberg metadata JSON,
     which transitively references manifest-list and manifest `.avro` files.
     These are NOT in `data_file` and must never be diffed (see below).
- **Path shape.** Mirror paths are absolute URIs (`s3://bucket/key` /
  `file://…`); LIST yields store-relative `Path` keys. `S3Storage::key_of`
  (`s3_storage.rs:129-137`) shows the normalization.

## Design

### Job kind + plumbing

New `control-plane/core` module `orphan_sweep.rs` mirroring `gc.rs`:

```rust
pub const ORPHAN_SWEEP_JOB_KIND: &str = "sweep_orphans";
/// Warehouse-scoped: no table payload.
#[derive(Serialize, Deserialize)]
pub struct OrphanSweepJob {}
```

Wiring (the enumerated new-kind checklist):

1. Add to `KNOWN_JOB_KINDS` (`core/src/queue.rs:32-41`).
2. Add to `SCHEDULABLE_JOB_KINDS` + an `OrphanSweepJob` decode arm in
   `validate_job_schedule` (`core/src/job_schedule.rs`).
3. Worker: add the kind to the dequeue array (`worker/src/main.rs:91-100`) and
   a dispatch arm → `handle_sweep_orphans` (mirrors `handle_gc`: deserialize,
   one `engine.sweep_orphans()` RPC, Abandon/Retry split via `run_wire_job`).
4. Engine: new `EngineControl` RPC `SweepOrphans(SweepOrphansRequest{}) ->
   SweepOrphansResponse { objects_deleted: u64, bytes_deleted: u64,
   candidates_skipped_grace: u64 }` (proto edit only — the `:pb-gen` genrule
   regenerates stubs), served by
   `iceberg_gc::sweep_orphans(&catalog, &pool, &store, grace)`.
5. `schedule_table_check` is untouched (no-table kinds fall through by
   construction) — pin that with a test rather than code.

No `/maintenance/...` HTTP enqueue endpoint in v1: the sweep's consumers are
schedules; an operator can create a one-shot schedule. (An HTTP trigger is a
one-line follow-on if wanted.)

### The sweep algorithm (`iceberg_gc::sweep_orphans`)

Ordering is **LIST first, then read references**: an object committed while
the sweep runs appears in the (later-read) reference set; an object *written*
mid-sweep is younger than the grace window. Both races resolve safe.

1. **LIST** the warehouse root (`store.list(None)` under `root_url`),
   collecting `(path, last_modified)` for objects matching the **data-pattern
   scope only**: `*.parquet` and Puffin blob paths (the path shape
   `vector_index.puffin_path` rows use). Everything else — Iceberg metadata
   JSON, manifest lists, manifest `.avro`, unknown files — is **excluded by
   scope, not diffed**: the sweep can never flag what it never considers.
2. **Read the reference set** from Postgres: every `data_file.path`
   (no `end_snapshot` filter — live and still-retained historical rows alike)
   and every `vector_index.puffin_path`, across **all** tables including
   dropped incarnations not yet fully reclaimed. Normalize both sides to
   store-relative keys against `root_url` before comparison
   (`key_of`-style stripping).
3. **Diff**: candidates = listed − referenced.
4. **Grace filter**: delete only candidates with
   `last_modified < now() - grace`. Grace default 24h
   (`LOOM_ORPHAN_SWEEP_GRACE_SECS`), guarding the write-then-commit race — an
   in-flight writer's uncommitted file is hours old at most.
5. **Delete** each survivor via the store handle, logging one `tracing` event
   per deletion (path, size, age) plus a summary; count failures without
   aborting the sweep (a failed delete is retried by the next scheduled run —
   idempotent by construction).

Concurrency: the sweep takes **no per-table advisory locks** — it never
touches the mirror, and the LIST-before-read ordering plus grace make
concurrent writers/GC safe. Two concurrent sweeps are harmless (deletes are
idempotent; `delete_file` treats missing objects as success —
`commit_mirror.rs:130-132`), and the schedule-firing dedup
(`pg_insert_if_absent`) prevents pile-up anyway.

### Safety posture (why this cannot delete a live file)

The failure that matters is a reference-diff bug deleting live data. Layered
guards:

1. **Pattern scoping** — metadata/manifests are structurally unreachable.
2. **Reference over-approximation** — the set includes *every* `data_file`
   row (any `end_snapshot`), so historical-but-retained files are referenced.
3. **LIST-before-read ordering** — a commit racing the sweep lands its
   reference before the diff reads.
4. **Grace window** — an uncommitted in-flight file is younger than grace.
5. **Observability** — per-deletion logs + RPC counts make a bad sweep loud
   and diagnosable.

### Knobs

| Knob | Env | Default | Where |
|---|---|---|---|
| Grace window | `LOOM_ORPHAN_SWEEP_GRACE_SECS` | 86400 (24h) | engine config (`run.rs`), threaded into `EngineControlService` like `retention` |

## Non-regression

- No existing path changes: the sweep is a new job kind; `gc_table` and its
  commit-then-delete contract are untouched.
- No migration: the reference reads use existing tables.
- New reference-set SQL is compile-time `query!` → `tools/sqlx-prepare.sh` +
  committed `.sqlx`.

## Testing

Fixture tests via `loom_fixture_test` (MinIO + Postgres already in the shared
fixture env), mirroring `postgres/tests/iceberg_gc.rs`; worker e2e mirroring
`scheduled_maintenance_e2e.rs`.

- **Live files survive** — land tables (multi-file + flush + puffin index),
  plant orphans, sweep: only orphans older than grace deleted; every
  referenced object still present; reads still green.
- **Historical-but-retained files survive** — end-cap files via COW overwrite
  within retention; sweep deletes none of them.
- **Metadata is untouchable** — Iceberg metadata JSON/`.avro` present under
  the root are never candidates (pattern scope), even when unreferenced by
  the mirror.
- **Grace holds young orphans** — a planted orphan younger than grace
  survives and is counted in `candidates_skipped_grace`.
- **Dropped-incarnation staged reclaim** — files of a dropped-but-in-window
  table are still referenced (their `data_file` rows survive until
  `gc_table`'s drop-snapshot aging) and survive the sweep.
- **Schedule e2e** — an `/admin/schedules` entry with kind `sweep_orphans`
  and `{}` payload validates, fires, dedups, and the worker drains it over
  the wire (extend `scheduled_maintenance_e2e.rs`).
- **Idempotence** — a second immediate sweep deletes nothing and errors
  nothing.

## Global constraints (loom-specific, carry into the plan)

- Strict clippy (pedantic + restriction): no `unwrap`/`expect`/`panic` in
  production code; `#[expect(lint, reason = "...")]` locally.
- `tools/sqlx-prepare.sh` + commit `.sqlx` after SQL changes.
- `buck2 run //tools:prek -- run --all-files` before every commit.
- **No object-store I/O inside a Postgres tx** — the sweep never opens one
  around deletes (reads references, then deletes outside any tx).
- Tests are `rust_test`/`loom_fixture_test` targets only, never inline
  `#[cfg(test)]`.

## Out of scope (deferred)

- **Dry-run / report-only mode** — add if a deployment wants a preview pass.
- **HTTP enqueue endpoint** — schedules are the v1 surface.
- **Manifest-graph walking** — the sweep never interprets Iceberg metadata;
  scoping excludes it instead. Full metadata-file GC (pruning old
  `previous_metadata_location` chains) is a separate future concern.
- **Per-prefix sharding / incremental LIST** — a whole-warehouse LIST is fine
  at current scale; shard when a warehouse makes one LIST impractical.

## Acceptance

1. Planted orphans (unreferenced `.parquet`/Puffin older than grace) are
   deleted by a scheduled `sweep_orphans` run; counts surface in the RPC
   response and logs.
2. No referenced object — live, historical-in-window, dropped-in-window, or
   metadata — is ever deleted (the fixture suite proves each class).
3. Young orphans survive until grace passes.
4. Existing suites green (`buck2 test //src/...`).

## Interfaces (names the plan consumes)

- Consumes: `store.list`/`ObjectMeta` (object_store); `WriteStore`/
  `ObjectStoreConfig::parse` (`store-config/src/lib.rs:94-236`);
  `pg_insert_if_absent` (`postgres/src/queue.rs`); `SCHEDULABLE_JOB_KINDS` /
  `validate_job_schedule` (`core/src/job_schedule.rs`); `KNOWN_JOB_KINDS`
  (`core/src/queue.rs:32-41`); worker dispatch (`worker/src/main.rs:91-128`);
  `EngineControlService` (`engine/src/service.rs`); `iceberg_mirror.data_file`
  / `vector_index` (migrations 0012/0018).
- Produces:
  - `ORPHAN_SWEEP_JOB_KIND = "sweep_orphans"` + `OrphanSweepJob {}` in
    `control_plane_core::orphan_sweep`.
  - `pub async fn sweep_orphans(catalog: &SqlCatalog, pool: &PgPool, store:
    &WriteStore, grace: Duration) -> Result<SweepSummary>` in
    `control_plane_postgres::iceberg_gc` (or a sibling module), with
    `SweepSummary { objects_deleted, bytes_deleted, candidates_skipped_grace }`.
  - `EngineControl::SweepOrphans` RPC + `EngineWireClient::sweep_orphans`.
  - Worker `handle_sweep_orphans` + dispatch arm.
  - `LOOM_ORPHAN_SWEEP_GRACE_SECS` on the engine config.
