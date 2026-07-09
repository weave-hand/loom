# Automatic compaction triggering (event-driven at commit) Design

> **Status:** design (direction). This spec makes `road-compaction-auto-trigger`
> build-ready (promoted 2026-07-09 from `#fut-compaction-auto-trigger`). A
> separate work agent writes the implementation plan from it and builds it.

## Problem

`#road-compaction-job` shipped compaction as **operator-triggered only**: an
explicit `POST /tables/{schema}/{table}/compact` (`ingest/src/http.rs:123`,
handler `:142-163`) enqueues one `compact_table` job, and the zero-pool worker
(`worker/src/compact.rs:23`) lists the table's live files over the wire, picks
the ones under a size cutoff (`small_files`, `core/src/catalog.rs:43` —
`file_size_bytes < threshold_bytes`), coalesces them via Flight +
`write_dataset`, and commits the swap over `EngineControl::CompactTable`
(`engine/src/service.rs:233` → `iceberg_compact::compact_table`,
`postgres/src/iceberg_compact.rs:22`).

Nothing *watches* for small-file accrual. Every flush of a 64 MiB inline
buffer, every multi-file land, every transform commit can add files under the
128 MiB cutoff, and they sit there degrading scan fan-out until an operator
notices. Meanwhile the inline tier already solved the same shape of problem
twice: the **byte-trigger flush** (`inline_append` step 3b,
`iceberg_inline.rs:701-735`) and the **CDC consolidate trigger**
(`iceberg_inline.rs:1211-1230`) both check a threshold on the write's own
commit transaction and enqueue a deduped maintenance job atomically with the
data. Compaction deserves the same treatment.

## Decision record

**Operator decision, 2026-07-09 (committed — do not relitigate):**
**event-driven at commit.** The small-file count is checked on the write/flush
commit seam, and a deduped `compact_table` job is enqueued when the count
crosses threshold N — mirroring the inline-flush trigger. NOT a periodic
sweep: a scheduled-maintenance-jobs mechanism is being specced separately as
`road-scheduled-maintenance-jobs`; a scheduled sweep can later ride it as a
*backstop* for tables that stop receiving commits, but it is out of scope
here.

## Context — what ships today (verified)

- **The trigger to mirror.** `inline_append` (`iceberg_inline.rs:701-735`)
  accrues live inline bytes via `bump_inline_trigger`
  (`iceberg_mirror.rs:532`, upsert on `iceberg_mirror.inline_trigger`,
  per-table `COALESCE(threshold, $global)` override), and on
  `live_bytes >= effective && !enqueued` enqueues one `flush_table` job and
  arms the debounce (`arm_inline_trigger`, `iceberg_mirror.rs:562`); the flush
  job resets it (`reset_inline_trigger`, `iceberg_mirror.rs:576`). It
  *suppresses* the enqueue for a table carrying inline shadow deltas
  (`has_shadow`, `iceberg_inline.rs:784` — the `iceberg_mirror.shadow_flag`
  table) because draining would flush a row-version/tombstone into Parquet and
  duplicate/resurrect a file row.
- **Deduped enqueue.** `pg_insert_if_absent` (`queue.rs:37-65`) inserts a job
  only if no `state = 'available'` job with the same `(kind, payload)` already
  exists; the `pg_notify` fires inside the CTE so it is buffered until the
  caller's tx commits. This is already the atomic-enqueue point of
  `CommitExtras.jobs` (`commit_mirror.rs:89-91`). Note the operator endpoint
  uses plain `enqueue` (`pg_insert`) and is NOT deduped — the auto-trigger
  must use `pg_insert_if_absent` and build the payload from the same
  `CompactJob { schema, name }` struct (`core/src/compact_job.rs:10`) so auto
  and operator jobs dedup coherently against a pending auto job.
- **The job and its selection cutoff.** `handle_compact`
  (`worker/src/compact.rs:23-93`) selects `small_files(&live,
  ctx.threshold_bytes)` and **no-ops when fewer than 2 qualify**
  (`compact.rs:40-42` — the convergence backstop). `threshold_bytes` comes
  from `LOOM_COMPACT_THRESHOLD_BYTES`, default 128 MiB
  (`worker/src/main.rs:50-51`). **The trigger must count files under this
  same cutoff** — the trigger predicts exactly the set the worker will act
  on.
- **Where new data files appear.** Every commit that adds data files lands
  rows in `iceberg_mirror.data_file` (path, `file_size_bytes`,
  `begin_snapshot`, `end_snapshot` — `migrations/0012_iceberg_mirror.sql:46`;
  live = `end_snapshot is null`), through exactly **three commit tails**:
  1. **The CAS tail** — `SqlCatalog::commit_mirror_in_tx`
     (`commit_mirror.rs:345`) → `write_mirror` (`:145`) → `project_files`.
     Reached by `do_update_table` (`:208`) and `do_update_table_in_tx`
     (`:307`). Covers: the ingest multi-file land
     (`append_parquet_snapshot`, `iceberg_landing.rs:257`), the flush commit
     (`iceberg_flush.rs:166`, engine-side via the `FlushTable` RPC,
     `engine/src/service.rs:121`), COW overwrite
     (`overwrite_parquet_snapshot`, `iceberg_landing.rs:1117`), and the
     stream direct write (`land_parquet_stream`, `iceberg_landing.rs:890`).
  2. **The mirror-only additive tail** — `land_additive`
     (`iceberg_landing.rs:423`, the schema-evolution additive branch of
     `land`, dispatched at `:315`) → `register_files(WriteMode::Append)`.
  3. **The mirror-only transform tail** — `IcebergTx::commit`
     (`iceberg_control_plane.rs:115-193`), which registers staged
     Append/Overwrite files (`:151-165`) and staged compactions (`:167-179`)
     — the seam behind the engine's `CommitTransform` RPC
     (`engine/src/service.rs:276-321`).
- **Compaction's own commit** goes through `register_files` with
  `WriteMode::Compact` only (`iceberg_compact.rs:38-48`;
  `IcebergTx::compact_files` → `staged_compacts`) — a *distinct* code path
  from the file-adding tails above, which is what makes the loop guard
  structural (below).
- **File sizes are already at hand at commit.** The CAS tail's
  `ProjectedFile.file_size_bytes` is read from the staged manifests during
  staging (`iceberg_mirror.rs:493-499`); the register tails map it straight
  off the loom `DataFile` (`projected_files`, `iceberg_landing.rs:774-796`).
  And the mirror *already knows every prior live file's size* — so the trigger
  needs no new state at all: it is one indexed `COUNT(*)` over
  `iceberg_mirror.data_file` per commit.

## Design

### One helper, three call sites, config on the `SqlCatalog`

A new postgres-crate helper is the whole trigger:

```rust
/// iceberg_compact.rs
pub struct CompactTriggerCfg {
    /// Files strictly smaller than this count toward the trigger — the SAME
    /// cutoff the worker's small_files selection uses (LOOM_COMPACT_THRESHOLD_BYTES).
    pub small_file_bytes: i64,
    /// Live small-file count at/above which a compact_table job is enqueued
    /// (LOOM_COMPACT_TRIGGER_FILES). Validated >= 2 at config parse.
    pub min_small_files: i64,
}

pub async fn maybe_enqueue_compact(
    conn: &mut PgConnection,
    table: &TableRef,
    cfg: &CompactTriggerCfg,
) -> Result<Option<JobId>>
```

Semantics (all inside the caller's commit tx, after the new files are
projected):

1. Resolve the live `table_id`; a never-written table is `Ok(None)`.
2. **Eligibility guards** (one combined query — see below): skip declared
   stream tables, changelog tables, and shadow-flagged tables.
3. Count live small files:
   `count(*) from iceberg_mirror.data_file where table_id = $1 and
   end_snapshot is null and file_size_bytes < $small_file_bytes` (served by
   the existing `iceberg_data_file_live_idx`; per-table live-file counts are
   small).
4. If `count >= min_small_files`, `pg_insert_if_absent` a
   `NewJob { kind: COMPACT_JOB_KIND, payload: json(CompactJob{schema,name}),
   run_at: None, priority: 0 }` — the identical payload shape the operator
   endpoint builds, so the two producers dedup against each other's pending
   job. The buffered `pg_notify` wakes the worker only if the commit lands.

The config rides on the **`SqlCatalog`** as
`compact_trigger: Option<CompactTriggerCfg>` (builder method
`SqlCatalogBuilder::with_compact_trigger`, precedent
`with_storage_factory`, `catalog.rs:68`; plus a consuming
`SqlCatalog::with_compact_trigger` setter for tests). `None` — the default —
disables the trigger entirely, preserving today's behavior byte-identically
for every existing caller and fixture (the same `Option = disabled` contract
as `inline_append`'s `flush_threshold`). This is the key ripple-avoider:
**every commit tail already holds the catalog**, so no landing/flush/transform
signature changes at all — the engine (`run.rs:113-118`), ingest
(`serve.rs:58-70`), fixture (`fixture.rs:551-560`) and seed
(`src/testing/seed.rs:216`) construction sites are the only wiring points.

Call sites (the three file-adding commit tails, enumerated above):

1. **`SqlCatalog::write_mirror`** (`commit_mirror.rs:145`) — at the end,
   after `project_files` + `stamp_schema_version`, when
   `self.compact_trigger` is `Some`. One call covers every CAS commit:
   ingest multi-file land, flush, COW overwrite, stream direct write.
2. **`land_additive`** (`iceberg_landing.rs:423`) — after its
   `register_files(WriteMode::Append)` (`:441`), reading the cfg off its
   `catalog` param.
3. **`IcebergTx::commit`** (`iceberg_control_plane.rs:115`) — once per
   distinct table in `staged_files` (the `written` vec it already builds for
   data triggers, `:187-189`), reading the cfg off `self.catalog`.
   **Deliberately NOT for `staged_compacts`.**

`inline_append` itself is untouched: inline commits add *rows*, not files —
the file appears at flush time, and the flush commit (tail 1) is where the
trigger sees it.

### Eligibility guards (why each)

- **Declared stream tables** (`stream.stream_table.table_id = tid`): a CDC
  base's current-state fold has its *own* trigger and job
  (`consolidate_threshold` → `stream_consolidate`,
  `iceberg_inline.rs:1211-1230`; `consolidate_stream` in engine-serving), and
  log/CDC files carry offset framing whose physical rewrite path is
  unproven under `compact_table`. Out of this slice; a stream small-file
  story is a follow-on.
- **Changelog tables** (`stream.stream_table.changelog_table_id = tid`,
  column from `migrations/0037_stream_table_cdc_columns.sql`): same
  reasoning — they are the durable CDC history, written by the dual flush.
- **Shadow-flagged tables** (`iceberg_mirror.shadow_flag`,
  `iceberg_inline.rs:784`): compaction re-projects surviving rows into files
  with a **new, higher `begin_snapshot`**, and the COW merge-on-read lets the
  highest `begin_snapshot` win per identity (`iceberg_inline.rs:757-762`) —
  compacting under live inline deltas could therefore resurrect tombstoned or
  superseded rows. This mirrors exactly the flush trigger's shadow
  suppression (`iceberg_inline.rs:707-724`). The tombstone-aware fold is the
  **sibling item `road-cow-compaction-consolidation`** (specced in parallel,
  `2026-07-09-cow-compaction-consolidation-design`); this trigger deliberately
  does not cover COW-delta consolidation.

Observation for the registers (not fixed here): the *operator* endpoint
enqueues `compact_table` for any table with none of these guards — a POST
against a shadow-flagged table is a pre-existing footgun that belongs to the
sibling item's scope; record it as an ISSUES entry when closing this one.

### Knobs

| Knob | Env | Default | Where |
|---|---|---|---|
| Small-file cutoff | `LOOM_COMPACT_THRESHOLD_BYTES` | 128 MiB | already the worker's selection knob (`worker/src/main.rs:50-51`); now ALSO read by engine `EngineTuning` (`engine/src/run.rs:33-92`) and ingest `RoutingTuning` (`ingest/src/config.rs:36-107`) for the trigger's count |
| Trigger file count N | `LOOM_COMPACT_TRIGGER_FILES` | 8 | new on `EngineTuning` + `RoutingTuning`; `0` disables the trigger (catalog built with `None`); any other value must be `>= 2` (fail startup naming the key, the `RoutingTuning::validate` pattern) |

Reusing the worker's env name for the cutoff is deliberate: one deploy value
governs both the trigger's counting and the worker's selection, so they agree
by default (the Helm chart sets it once per environment). If they ever drift,
the system stays safe — a mismatch only makes the trigger over- or
under-eager; the worker re-derives its own selection from live state and
no-ops below 2.

Default rationale: flush emits ~64 MiB files (`LOOM_FLUSH_BYTE_THRESHOLD`
default), so every flush file is "small" under the 128 MiB cutoff; N = 8
compacts roughly every 512 MiB of flushed throughput, and the coalesced
output (~512 MiB, split by `write_dataset`'s size-estimated repartition)
exits the small set — genuine convergence, not churn.

Unlike the byte trigger there is **no per-table threshold override column**
in v1 (the `inline_trigger.threshold` analog): the count is stateless, so an
override can be added later without migration coupling. Deliberately
deferred.

### Loop guard / back-pressure guarantee

The register prose worried about "the policy and back-pressure story". The
guarantees, as verified in code:

1. **Compaction's own commit cannot re-trigger — structurally.** The trigger
   helper is called only from the three file-*adding* tails. Compaction
   commits through `register_files(WriteMode::Compact)`
   (`iceberg_compact.rs:38-48`) and `IcebergTx`'s `staged_compacts` loop
   (`iceberg_control_plane.rs:167-179`) — neither calls the helper. This is
   not a threshold accident (the output usually exceeding the cutoff); it is
   a code-path property, so even a pathological compaction that emits small
   outputs cannot enqueue from its own commit.
2. **At most one queued job per table.** `pg_insert_if_absent`
   (`queue.rs:37-65`) admits one `state='available'`
   `(compact_table, {schema,name})` row at a time. While that job is
   *running*, at most one more can queue behind it — which is the desired
   behavior (files landed after the dequeue need a later pass), and the
   steady-state bound is one available + one running per table.
3. **Convergence.** The worker no-ops below 2 small files
   (`compact.rs:40-42`), each run strictly reduces the table's small-file
   count (k >= 2 coalesced into fewer, larger files), and with
   `min_small_files >= 2` enforced, re-arming requires N *new* small files
   from genuine writes. No trigger-state table, no arm/reset protocol to
   corrupt — the debounce IS the queue state.
4. **Write-path cost is bounded.** One indexed count (+ the guard subqueries,
   one round trip) per file-adding commit, inside a tx that already did
   multi-statement mirror projection. No object-store I/O (the
   `iss-iceberg-tx-objectstore` invariant is untouched — the helper is pure
   Postgres). If the worker pool falls behind, jobs dedup rather than pile
   up; the writer is never blocked on compaction.

### Mechanism note — stateless by design (a register-prose correction)

The register says "mirroring the inline-flush trigger". What is mirrored is
the **seam and contract** — evaluated on the write's own commit tx, atomic
with the data, deduped enqueue, `pg_notify` buffered to commit, disabled by
`None` — not the mechanism. The flush trigger must keep a running byte
counter (`inline_trigger.live_bytes`) because inline bytes are not cheaply
re-derivable per commit; the compact trigger's input (live small-file count)
already sits fully materialized in `iceberg_mirror.data_file`, so it carries
**no trigger-state row, no arm/disarm, no reset-in-the-job**. Less state,
fewer failure modes.

## Non-regression

- `compact_trigger: None` (the default) makes every existing path
  byte-identical — no existing test may change behavior. Only the engine and
  ingest mains (and the new tests) opt in.
- The worker, the operator endpoint, `CompactJob`, and
  `iceberg_compact::compact_table` are unchanged (the worker gains no new
  code — it already handles the job kind).
- No migration: the count reads existing mirror state; the guards read
  existing tables.
- New SQL (the eligibility+count query) is compile-time `query!` →
  `tools/sqlx-prepare.sh` + committed `.sqlx` (cloud sessions fall back to
  `AssertSqlSafe`, the `has_shadow` precedent, `iceberg_inline.rs:769-793`).

## Testing

Tests are `rust_test` / `loom_fixture_test` integration targets only — never
inline `#[cfg(test)]`. New fixture tests use `loom_fixture_test`
(`src/control-plane/postgres/defs.bzl`) and mirror the harnesses of
`tests/inline_flush_trigger.rs` (trigger shape) and
`worker/tests/compact_e2e.rs` (compaction e2e).

- **Crossing enqueues exactly one** — land N small Parquet files through
  `land` against a trigger-configured catalog; assert exactly one
  `state='available'` `compact_table` job with the `{schema,name}` payload.
- **Below threshold never enqueues** — N−1 small files ⇒ zero jobs.
- **Dedup** — keep landing past N ⇒ still one available job.
- **Large files don't count** — N files at/over the cutoff ⇒ zero jobs.
- **Compaction's commit does not re-trigger** — expire the smalls via
  `iceberg_compact::compact_table` (registering one coalesced file) ⇒
  available-job count unchanged.
- **Guards** — a declared stream table, a changelog table, and a
  shadow-flagged table each accrue >N small files ⇒ zero jobs.
- **Disabled** — a `None`-config catalog ⇒ zero jobs (the default-off
  regression pin).
- **Transform tail** — an `IcebergTx` `append_files` commit crossing N ⇒
  one job; a `compact_files` commit ⇒ none.
- **End-to-end acceptance** (worker crate) — land >N small files (auto-job
  appears with no operator POST), drain it with `handle_compact` over the
  engine wire, assert the files coalesced, the rows are intact, and no new
  available job exists after compaction's commit.
- **Knob parsing** — `EngineTuning`/`RoutingTuning`: defaults, overrides,
  `0` disables, `1` fails validation naming `LOOM_COMPACT_TRIGGER_FILES`.

## Global constraints (loom-specific, carry into the plan)

- **Clippy is strict (pedantic + restriction):** no `unwrap`/`expect`/
  `indexing_slicing`/`panic`/`todo` in production lib/bin code;
  `#[expect(lint, reason = "...")]` for justified local exceptions. Test code
  is exempted from the panic-safety lints via
  `loom_rust_test`/`loom_fixture_test`.
- **After changing any `query!`/`query_scalar!` SQL**, run
  `tools/sqlx-prepare.sh` and commit `.sqlx/`; the `sqlx-cache-check` test
  enforces freshness.
- **Run prek before every commit:** `buck2 run //tools:prek -- run
  --all-files`. Markdown ends with exactly one trailing newline, no trailing
  whitespace.
- **The enqueue must be fire-inside-the-commit-tx** — never a code path that
  can fail the write independently; `pg_insert_if_absent`'s CTE shape
  (`queue.rs:46-60`) already guarantees the notify/insert are atomic with the
  caller's commit.
- **No object-store I/O inside the commit tx** (`iss-iceberg-tx-objectstore`)
  — the helper is Postgres-only by construction.

## Out of scope (deferred)

- **Periodic sweep** — a scheduled small-file sweep (catching tables that go
  quiet below the event-driven seam) is a natural rider on
  `road-scheduled-maintenance-jobs`, specced separately. Mentioned as a later
  complement only.
- **COW inline-delta consolidation triggering** — the sibling
  `road-cow-compaction-consolidation` (tombstone-aware fold of inline shadow
  deltas, spec `2026-07-09-cow-compaction-consolidation-design`) owns
  shadow-flagged tables; this trigger skips them and does not enqueue that
  job kind.
- **Stream/changelog small-file compaction** — skipped by the guards;
  CDC current-state folding is `consolidate_stream`'s job (own trigger).
- **Per-table trigger overrides** (the `inline_trigger.threshold` analog) —
  stateless v1; add a config column later if a real need appears.
- **Guarding the operator endpoint** with the same eligibility checks —
  pre-existing surface, recorded as an ISSUES entry at close, owned by the
  sibling/stream items.
- **Watermark-incremental compaction** (`fut-compaction-watermark`) — the
  worker's selection strategy, orthogonal to when it is triggered.

## Acceptance

1. A table accruing more than N sub-cutoff files via ordinary commits (land /
   flush / transform) gets **exactly one** auto-enqueued `compact_table` job
   — no operator POST involved.
2. Compaction's own commit does **not** re-trigger (available-job count
   unchanged after the swap commits).
3. Below-threshold tables — and stream/changelog/shadow-flagged/disabled
   tables at any count — **never** enqueue.
4. Existing suites green and byte-identical with the trigger unconfigured
   (`buck2 test //src/...`).

## Interfaces (names the plan consumes)

- Consumes: `pg_insert_if_absent` (`queue.rs:37`); `COMPACT_JOB_KIND` /
  `CompactJob` (`core/src/compact_job.rs:6-13`); `small_files` cutoff
  semantics (`core/src/catalog.rs:43`); `live_table_id`
  (`iceberg_mirror.rs:264`); `pg_stream_meta` (`stream.rs`); `has_shadow`
  (`iceberg_inline.rs:784`); `iceberg_mirror.data_file`
  (`migrations/0012`); `write_mirror` (`commit_mirror.rs:145`);
  `land_additive` (`iceberg_landing.rs:423`); `IcebergTx::commit`
  (`iceberg_control_plane.rs:115`); `SqlCatalogBuilder` (`catalog.rs:68`);
  `EngineTuning::from_map` (`engine/src/run.rs:58`);
  `RoutingTuning::overlay_env`/`validate` (`ingest/src/config.rs:65-107`);
  `handle_compact` (`worker/src/compact.rs:23`).
- Produces (the plan relies on these EXACT names/types):
  - `pub struct CompactTriggerCfg { pub small_file_bytes: i64, pub
    min_small_files: i64 }` and `pub async fn maybe_enqueue_compact(conn:
    &mut PgConnection, table: &TableRef, cfg: &CompactTriggerCfg) ->
    Result<Option<JobId>>` in `control_plane_postgres::iceberg_compact`.
  - `SqlCatalog.compact_trigger: Option<CompactTriggerCfg>` +
    `SqlCatalogBuilder::with_compact_trigger(CompactTriggerCfg)` +
    `SqlCatalog::with_compact_trigger(self, CompactTriggerCfg) -> Self`.
  - Trigger evaluation at the end of `write_mirror`, in `land_additive`, and
    per written table in `IcebergTx::commit` (never for
    `WriteMode::Compact` / `staged_compacts`).
  - `EngineTuning { compact_small_file_bytes, compact_trigger_files, .. }`
    (`LOOM_COMPACT_THRESHOLD_BYTES` / `LOOM_COMPACT_TRIGGER_FILES`) and the
    same pair on `RoutingTuning`.
