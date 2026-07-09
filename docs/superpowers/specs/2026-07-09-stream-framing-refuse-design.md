# Refuse stream-table targets on the legacy transform write paths — Design

> **Status:** design (direction). This spec makes `road-stream-framing-write-paths`
> build-ready (promoted 2026-07-09 from `#fut-stream-framing-write-paths`). A
> separate work agent writes the implementation plan from it and builds it.
> Must land **before or with** stream slice 4 (`road-stream-continuous`).

## Problem — the latent corruption class

A declared stream table's (log or CDC) physical schema carries the three
reserved framing columns — `loom_change_kind` / `loom_bucket` / `loom_offset` —
and every read that makes streams work (offset-ordered folds, merge-on-read
`Precedence::Offset`, consolidation, the future subscribe/MV delta scans)
assumes they are present and populated. The framing-aware write paths ship:
flush derives `include_framing` from the stream registry
(`iceberg_flush.rs:155`), the direct large-write landing path stamps it, and —
as of the CDC slice — `overwrite_parquet_snapshot` derives it too
(`iceberg_landing.rs:1133-1141`, the pattern this work's *lookup* mirrors).

Two write paths remain **framing-unaware** — they hardcode
`include_framing=false` and register framing-free column sets:

- the multi-target `iceberg_landing::write_steps` commit
  (`iceberg_landing.rs:519`; the hardcoded `false` + "out of this slice's
  scope" comment at `:555-556`), and
- the transform-commit staging seam `IcebergTx::commit` in
  `iceberg_control_plane.rs` (`:115`; the hardcoded `false` at `:142-144`,
  staged `append_files`/`replace_files` registered at `:151-164`).

If either writes a declared stream table, the failure mode depends on whether
the table's mirror schema already projects framing (framing enters the mirror
on the first framed Parquet append — i.e. the first flush):

- **After the first flush** (mirror projects `loom_*`): the framing-free
  incoming column set classifies as a dropped-column schema change and the
  commit fails with the opaque `schema evolution unsupported: column
  "loom_change_kind" was dropped` (`classify_schema_change`,
  `iceberg_schema_evolution.rs:45`, `ColumnDropped`). Loud, but misleading —
  it reads as a schema bug, not "you targeted a stream table".
- **Before the first flush** (declared stream table, inline rows only, mirror
  projects user columns): the framing-free write **classifies as `Identical`
  and commits silently**. The stream table now holds file rows with no
  `loom_bucket`/`loom_offset` — offset-based folds, consolidation, and (once
  built) subscribe/MV reads over those rows are silently wrong. This is the
  real corruption window.

Neither path is exercised against a stream table by any *sanctioned* flow
today — but both are **operator-config-reachable** (see per-site reachability
below), so "unreachable" undersells it: it is an accident waiting for the
first transform or multi-step action someone points at a CDC-bound table.

## Decision record — refuse, don't thread (operator, 2026-07-09)

**Decision:** the legacy transform/multi-target write paths get a stream-ness
check and return a typed `Validation`-class error for stream/CDC-registered
target tables (including CDC changelog tables). Framing is **not** threaded
through them.

**Rationale:** stream slice 4 (`road-stream-continuous`, spec
`2026-07-09-stream-continuous-design.md`) defines a dedicated
`EngineControl::CommitMicroBatch` RPC as the sanctioned stream-output path —
it lands MV output on the inline tier via `inline_append_decl`, which stamps
framing, allocates gapless per-bucket offsets in the commit tx, declares the
output table via `reconcile_stream_mode`, and CAS-advances the watermark
atomically. Threading framing through `write_steps`/`IcebergTx` would
duplicate all of that (offset allocation, bucket assignment, declaration
reconciliation) on paths whose only stream traffic would be accidents — pure
blast-radius enlargement. Refusal closes the corruption class with a handful
of small, byte-identical-for-batch-tables guards.

**Exit condition (the rejected alternative, kept findable):** if a future need
arises for transforms writing stream tables *directly* (e.g. the deferred
framed direct-Parquet MV output path noted in the slice-4 spec's non-goals),
the refusal sites enumerated below are exactly the places framing threading
would go — `write_steps` Phase 1's `ensure_iceberg_table` + per-`StepLand`
framing stamping/offset allocation, and `IcebergTx`'s staged creates/registers.
Deleting a guard and threading `include_framing` + offset allocation there is
the alternative shape rejected today.

## Guard sites (verified)

### The lookup (shared): "is this table stream-registered?"

Source of truth is the `stream.stream_table` registry
(`src/control-plane/postgres/src/stream.rs`): resolve
`live_table_id(conn, schema, name)` (`iceberg_mirror.rs:264`), then check
`stream.stream_table` — the same derivation `overwrite_parquet_snapshot`
already does via `pg_stream_bucket_count` (`iceberg_landing.rs:1133-1141`,
`stream.rs:330`). **One extension over that pattern:** a CDC table's durable
changelog (`<schema>.<name>__changelog`) has a mirror row but **no**
`stream_table` row of its own — it is pointed at by the base row's
`changelog_table_id` (`stream.rs:404`). A bucket-count lookup alone misses it,
and a transform overwriting the changelog would corrupt the durable log just
as badly. The guard therefore refuses when the resolved table id appears as
**either** `table_id` **or** `changelog_table_id` in `stream.stream_table`.

New helper (postgres crate, both commit sites and the define-time check are
in-crate): `pg_refuse_stream_target(conn: &mut PgConnection, table: &TableRef)
-> Result<()>` in `postgres/src/stream.rs`, returning
`ControlPlaneError::Validation` with the stable, matchable message prefix
`stream-table target refused:`. Runtime `AssertSqlSafe` for the one new query
(mirroring `version_for_table` — no `.sqlx` regen, which cloud sessions cannot
run). A table with no live mirror row passes (a brand-new output table cannot
be stream-declared).

### Site 1 — `iceberg_landing::write_steps` (`iceberg_landing.rs:519`)

Hardcodes `include_framing=false` at `:555-556`. Sole production caller chain:
query-api **multi-step actions** (`action.rs:1415` `run_multi_step`) →
`EngineControl::WriteSteps` (`engine/src/service.rs:387-420`) →
`IcebergActionWriter::write_steps` (`action_writer.rs:114-141`) →
`iceberg_landing::write_steps`. (Fixture tests call it directly:
`postgres/tests/data_triggers.rs:262`, `query-api/tests/write_steps_e2e.rs`.)

**Reachable today: yes.** Nothing restricts a multi-step `ActionDef` step's
target to non-stream types — single-step actions on a CDC-bound type route
through the stream-aware `write_delta` (`run_mutate`/`run_insert`), but the
multi-step path stages every step as an Append/Overwrite `StepWrite`
regardless of stream-ness. Note the register prose attributed this path to
"transforms"; its actual production caller is the multi-step *action* seam.

**Guard:** inside the Phase-2 commit transaction, before each target's
`register_files` (`:573-580`), call `pg_refuse_stream_target(&mut *tx, …)` per
staged target. In-tx placement makes the refusal atomic with the multi-target
commit: if *any* step targets a stream table, **nothing** lands (the
all-or-nothing contract multi-step actions already promise). Phase-1 Parquet
IO for a refused commit is wasted but harmless (unreferenced files, GC-able) —
an accident-only path does not justify a second pre-IO lookup.

### Site 2 — `IcebergTx::commit` (`iceberg_control_plane.rs:115`)

Hardcodes `include_framing=false` at `:142-144`; registers staged
`append_files`/`replace_files` at `:151-164`. Sole production caller: the
engine's `CommitTransform` handler (`engine/src/service.rs:276-321` —
`replace_files` at `:308`, `append_files` at `:310`), driven by the worker's
physical and typed transform jobs (`worker/src/transform.rs:348-365`).

**Reachable today: yes.** A `TransformDef` output is an arbitrary
`TableRef`/`TypeName`; neither `validate_transform_def`
(`core/src/transforms.rs:261`) nor the adapter checks stream-ness, so a def
whose output names a declared stream table commits through this seam on its
first run.

**Guard:** in `IcebergTx::commit`, inside the held transaction, before the
`staged_files` register loop, call `pg_refuse_stream_target(&mut *tx, …)` per
distinct staged-files table. `staged_compacts` are deliberately **not**
guarded — compaction is schema-invariant (columns `&[]`, files keep whatever
framing they carry), and the stream consolidate path legitimately rides
overwrite, not this seam.

### Site 3 — `Tx::replace_files` off the overwrite seam

The only production call site of the trait method is
`engine/src/service.rs:308` inside `CommitTransform` — covered by Site 2's
guard (the staging seam, not the trait method, is the chokepoint; guarding the
trait method would miss `append_files`, which corrupts identically). The
memory adapter's `replace_files` (`memory/src/transaction.rs:390`) is the test
fake and gets **no guard**: the memory control plane has no `TableRef →
table_id` mirror to resolve against (the same asymmetry the slice-4 spec
records for its run-time source validation). The COW UPDATE/DELETE and
consolidate overwrites ride `overwrite_parquet_snapshot`, which is already
framing-preserving — the *overwrite seam* needs no refusal.

### Define-time UX guard — `define_transform` (postgres adapter)

Precedent (from the data-triggers work, PR #373): the postgres
`define_transform` (`postgres/src/transforms.rs:298`) already performs
adapter-side, DB-backed define-time validation — the typed-refs-known check
(`:311-329`) and the trigger-cycle check (`:330-358`, resolving typed bodies
via `pg_type_tables` at `:353`). The stream-target check slots in beside them:
resolve the def's output `TableRef` (Physical: the literal output; Typed: the
output type's backing table via `pg_type_tables`) and call
`pg_refuse_stream_target` on the define transaction. A def whose output does
not exist yet passes (nothing to refuse — it cannot be stream-declared).

This layer is **UX, not authority**: it catches the misconfiguration at
`POST /admin/transforms` time with a 4xx instead of a first-run failure. The
commit-time guard remains the hard gate, because (a) ad-hoc runs skip
`define_transform` entirely, (b) a def can predate the output table's stream
declaration, and (c) a `define_type` rebind can re-point a typed def's output
at a stream table after define. The memory adapter's `define_transform` skips
the check (no mirror — same posture as its trigger machinery); the testkit
`transforms_contract` is therefore unchanged, and the new rejection is
asserted in a postgres-only fixture test.

## Error contract

The refusal is `ControlPlaneError::Validation` with message prefix
`stream-table target refused:` (stable and matchable, like
`schema evolution unsupported:`). Today's wire plumbing would mangle it, so
the contract includes four small mapping fixes:

- **Engine gRPC `status()`** (`engine/src/service.rs:21-28`): today
  `Validation` falls into `other => Status::internal`. Add
  `Validation(m) => Status::invalid_argument(m)`. This round-trips: the
  engine-wire client already maps `InvalidArgument →
  ControlPlaneError::Validation` (`engine-wire/src/client.rs:59`). Side
  effect (accepted, an error-fidelity improvement): other deterministic
  `Validation` faults on `EngineControl` (e.g. `write_steps: no targets`)
  also become `invalid_argument` instead of `internal`.
- **Worker taxonomy** (`worker/src/transform.rs:348-365`): the
  `commit_transform` step currently wraps **every** error in
  `JobFailure::retry` — a deterministic refusal would retry until exhaustion.
  Match `ControlPlaneError::Validation → JobFailure::abandon` (the
  `run_wire_transform` taxonomy the slice-4 spec also assumes: deterministic
  faults abandon, wire/store faults retry); everything else keeps retry. The
  run is then reported Failed-terminal via the existing `report_run_failure`.
- **Action path (engine-serving → query-api):**
  `IcebergActionWriter::write_steps` currently collapses every error to
  `EngineServingError::Engine(String)` (`action_writer.rs:138-140`), and the
  engine's `write_steps` handler blanket-maps to `Status::internal`
  (`service.rs:412-416`). Add `EngineServingError::Validation(String)`
  (`engine-serving/src/serving.rs:36-60`), constructed from
  `ControlPlaneError::Validation` in `action_writer::write_steps`; map it to
  `invalid_argument` in `serving_status` (`engine/src/flight.rs:38-47`) and
  via a narrow match in the `write_steps` handler (only the new variant —
  other errors stay `internal`, byte-identical).
- **Query-api surface:** the wire client maps `invalid_argument` back to
  `ControlPlaneError::Validation`; `to_serving_write`
  (`engine_action_client.rs:24-29`) gains a `Validation(m) →
  ServingError::Unsupported(m)` arm (new `ServingError` variant,
  `query-api/src/serving.rs:76-97`), and `run_multi_step` maps
  `ServingError::Unsupported → ActionError::Unsupported` — which the HTTP
  layer already renders as **422** with the message (`http.rs:1098-1100`). A
  multi-step action against a stream-bound target thus fails with a clear
  client error, never a 500, and nothing is written.

Define-time, no new plumbing is needed: `define_transform`'s `Validation`
already surfaces through the existing admin-transforms error mapping.

## Non-regression

Batch and log/CDC tables on their *sanctioned* paths are untouched:

- The guard adds one registry lookup per target **only** on `write_steps` and
  `IcebergTx::commit` transactions; for every non-stream table the lookup
  returns empty and behavior is byte-identical (same snapshots, same errors).
- Framing-aware paths (inline, flush, land, overwrite, `write_delta`,
  consolidate) are not modified.
- Existing suites must stay green unmodified: `transform-e2e`,
  `typed-transform-e2e`, `data-triggers` (its `write_steps` leg targets two
  plain tables), `multi_step_run`, `action_multi_object_e2e`,
  `write_steps_e2e`, the `stream_*` family, and the transforms testkit
  contracts (both adapters).
- The `status()`/handler error-mapping changes alter only which gRPC code
  carries already-failing responses; no success path changes.

## Acceptance

- **Define-time:** `define_transform` (postgres) of a Physical def whose
  output names a declared log or CDC table, and of a Typed def whose output
  type binds to one, returns `ControlPlaneError::Validation` carrying
  `stream-table target refused:`. A def whose output is a batch table or does
  not exist yet defines successfully (byte-identical).
- **Commit-time (transform e2e):** a transform defined while its output was
  undeclared, whose output table is *then* declared a stream table, fails at
  run time: the engine refuses the `CommitTransform` with `invalid_argument`,
  the worker **abandons** (no retry), and the run is reported Failed-terminal
  with the refusal message. Nothing is registered against the stream table
  (no new snapshot, no files).
- **Multi-target:** `iceberg_landing::write_steps` with any step targeting a
  declared stream table — or a CDC changelog table — returns `Validation` and
  commits **nothing** (a co-staged batch target's rows are absent; no
  snapshot allocated survives). A multi-step action driving the same seam
  over the wire surfaces HTTP 422.
- **Byte-identical elsewhere:** the same `write_steps`/transform flows against
  batch tables succeed exactly as before; the full existing suite passes
  unmodified.

## Out of scope (deferred / rejected)

- **Threading framing through the legacy paths** — the rejected alternative;
  see the decision record and exit condition above.
- **`EngineControl::CommitMicroBatch`** — the sanctioned stream-output path is
  slice 4's work (`road-stream-continuous`), not this item's; this item only
  keeps the legacy paths from competing with it.
- **A memory-adapter guard** — no `TableRef → table_id` mirror; the memory
  `Tx` fake stays guard-free (contract tests unchanged).
- **The overwrite seam's own residual** — `overwrite_parquet_snapshot` with a
  non-framed batch against a framed schema fails via `classify_schema_change`
  today; improving *that* message is not this item.
- **Guarding `compact_files`** — compaction is schema-invariant and safe.

## Interfaces (names the plan consumes)

- Consumes: `live_table_id` (`postgres/src/iceberg_mirror.rs:264`);
  `pg_stream_bucket_count`/`pg_stream_meta`/`stream.stream_table` +
  `changelog_table_id` (`postgres/src/stream.rs:330`/`:367`);
  `iceberg_landing::write_steps`/`StepLand` (`iceberg_landing.rs:519`/`:503`);
  `IcebergTx::commit` (`iceberg_control_plane.rs:115`); the engine handlers +
  `status()`/`serving_status` (`engine/src/service.rs:21`/`:276`/`:387`,
  `engine/src/flight.rs:38`); `IcebergActionWriter::write_steps`
  (`engine-serving/src/action_writer.rs:114`); `EngineServingError`
  (`engine-serving/src/serving.rs:36`); the worker commit step
  (`worker/src/transform.rs:348`); `define_transform` + `pg_type_tables`
  (`postgres/src/transforms.rs:298`/`:353`); `to_serving_write` /
  `ServingError` / `ActionError::Unsupported` → 422
  (`query-api/src/engine_action_client.rs:24`, `query-api/src/serving.rs:76`,
  `query-api/src/action.rs`, `query-api/src/http.rs:1098`); test harnesses
  `postgres/tests/stream_overwrite_framing.rs` (declare + flush fixture
  shape), `worker/tests/transform_e2e.rs` (engine-UDS worker e2e shape),
  `query-api/tests/write_steps_e2e.rs`.
- Produces (the plan relies on these EXACT names):
  - `pub(crate) async fn pg_refuse_stream_target(conn: &mut sqlx::PgConnection,
    table: &TableRef) -> Result<()>` in `postgres/src/stream.rs`
    (`AssertSqlSafe`; refuses `table_id` OR `changelog_table_id` hits; message
    prefix `stream-table target refused:`).
  - Guard calls in `iceberg_landing::write_steps` (Phase-2 tx) and
    `IcebergTx::commit` (held tx, staged-files tables only).
  - The define-time check in postgres `define_transform` (Physical + Typed
    output resolution).
  - `Validation(m) => Status::invalid_argument(m)` in `status()`;
    `EngineServingError::Validation(String)` + its `serving_status`/handler
    mapping; `ServingError::Unsupported(String)` + the `to_serving_write` and
    `run_multi_step` arms; the worker's abandon-on-`Validation` commit match.
