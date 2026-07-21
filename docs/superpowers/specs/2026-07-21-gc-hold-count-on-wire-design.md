# GC hold count on the wire Design

> **Status:** design (direction). This spec makes `road-gc-hold-count-on-wire` build-ready
> (promoted from `fut-gc-hold-count-on-wire`). A separate work agent writes the implementation
> plan from it and builds it.

## Problem

`gc_table` counts the candidates the MV read-position floor withheld —
`GcSummary.held_by_mv_floor` (`postgres/src/iceberg_gc.rs:84-96`; the summary's four `u64`
fields are `data_file_rows`, `inline_rows`, `objects_deleted`, `held_by_mv_floor`) — but the
count dies inside the engine process:

- The engine computes it (`engine/src/service.rs:393`) and then builds a three-field
  `GcTableResponse` (`:396-400`) that **drops it**.
- The proto (`engine-wire/proto/engine_control.proto:69`) has only
  `data_file_rows = 1; inline_rows = 2; objects_deleted = 3;`.
- The wire client returns a 3-tuple (`engine-wire/src/client.rs:264-273`).
- The worker's `handle_gc` **discards even that** (`worker/src/handler.rs:45-54`,
  `.map(|_| ())`), and query-api's `POST /maintenance/gc/{schema}/{table}` is a fire-and-forget
  202 `{job_id}` enqueue (`query-api/src/http.rs:554-570`) — no reclaim data anywhere.

So an operator watching GC sees "nothing was reclaimed" with no why. The engine-side
`tracing::warn!`s (`iceberg_gc.rs:192-213`: `held`, `floor`, `slowest`, and the stranded-MV
name set) are the sole signal for a wedged MV, ghost-watermark pinning, or the
pre-declaration-file hold (`[[iss-mv-floor-holds-pre-declaration-files]]`).

## Decision (operator, 2026-07-21): wire field + worker logs the counts

Scope: carry the **count** end-to-end and make the worker surface it in its logs. The laggard/
stranded MV **identity strings stay engine-log-only** (the engine warn already names them;
duplicating strings on the wire adds proto surface for information one hop away).

1. **Proto:** add `uint64 held_by_mv_floor = 4;` to `GcTableResponse`
   (`engine_control.proto:69`). Proto3-additive — old readers ignore it, old writers yield 0.
2. **Engine:** populate it from the already-computed summary (`engine/src/service.rs:396-400`).
3. **Wire client:** the 3-tuple return of `client::gc_table` (`client.rs:264-273`) is at its
   readability limit — replace it with a small named struct in `engine-wire` (e.g.
   `GcCounts { data_file_rows, inline_rows, objects_deleted, held_by_mv_floor }`), mirroring
   `GcSummary` but staying a wire-crate type (do not leak the postgres crate's type across the
   wire boundary). Update the callers: worker `handle_gc` and the four destructuring sites in
   `worker/tests/stream_mv_e2e.rs` (`:832-835, :871-876, :957, :1111-1112`).
4. **Worker:** `handle_gc` (`worker/src/handler.rs:45-54`) stops discarding: log the counts on
   completion — `tracing::info!` with all four counts (+ schema/table), and `tracing::warn!`
   when `held_by_mv_floor > 0` (the "GC ran but the floor held work back" operator signal,
   pointing at the engine log for the laggard identity). No behaviour change beyond logging.

Out of scope (unchanged): query-api's 202 enqueue response — surfacing counts over HTTP needs a
job-status/read surface that does not exist; that remains a separate future item if wanted.

## Testing

- **Wire round-trip:** extend the existing held-file scenario in
  `worker/tests/stream_mv_e2e.rs:829-855` — it already constructs a floor-held file and
  verifies the hold via DB/filesystem state; add the assertion that the wire call returns
  `held_by_mv_floor == <expected>` (the count's unit is candidates held: data_file rows +
  end-capped inline rows, per the `GcSummary` doc comment at `iceberg_gc.rs:89-94`). Also assert
  `held_by_mv_floor == 0` on a no-hold GC call at one of the other three sites.
- **Worker logging:** the worker e2e (`worker/tests/e2e.rs:326-403` or
  `scheduled_maintenance_e2e.rs`) can stay success-only; if cheap, assert the log line via the
  existing tracing-capture idiom — do not build new harness machinery for it.
- Existing direct-primitive tests (`postgres/tests/mv_floor.rs`, `tests/iceberg_gc.rs`) are
  unaffected.

## Non-regression

- Proto change is additive (field 4 unused today); no migration, no `.sqlx` change.
- `GcSummary` itself is untouched; the engine warn!s are untouched.
- Worker behaviour on GC failure is unchanged (only the success path gains logging).

## Global constraints (loom-specific, carry into the plan)

- Strict clippy (pedantic + restriction); `buck2 run //tools:prek -- run --all-files` before
  every commit.
- Engine-wire proto edits regenerate prost output at build time — no checked-in generated code
  to refresh; confirm by building `//src/services/engine-wire`.

## Acceptance

1. `GcTableResponse` carries `held_by_mv_floor`; the engine populates it; the wire client
   returns it as a named-struct field.
2. The worker logs all four counts per GC job and warns when the floor held candidates.
3. The `stream_mv_e2e.rs` held-file scenario asserts the count over the wire (held and
   zero cases).
4. Existing suites green (`buck2 test //src/...`).

## Interfaces (names the plan consumes)

- Consumes: `GcSummary` (`postgres/src/iceberg_gc.rs:84-96`), `GcTableResponse`
  (`engine-wire/proto/engine_control.proto:69`), engine populate site
  (`engine/src/service.rs:393-400`), wire client `gc_table` (`engine-wire/src/client.rs:264-273`),
  worker `handle_gc` (`worker/src/handler.rs:45-54`), e2e call sites
  (`worker/tests/stream_mv_e2e.rs:832,871,957,1111`).
- Produces: the fourth proto field, the `GcCounts` client struct, the worker logging, the wire
  held-count assertions.
