# Orphan-sweep grace-window boundary Design

> **Status:** design (direction). This spec makes `iss-orphan-sweep-zero-grace-boundary-flake`
> build-ready. The item stays in ISSUES (a defect in shipped code); a separate work agent writes
> the implementation plan from it and builds it.

## Problem

`sweep_orphans` (`src/control-plane/postgres/src/orphan_sweep.rs`) holds a deletion candidate
whose age has not passed the grace window with:

```rust
let now_ms: i64 = (OffsetDateTime::now_utc().unix_timestamp_nanos() / 1_000_000) as i64; // :144
let cutoff_ms = now_ms - grace.as_millis() as i64;                                       // :145
...
if modified_ms >= cutoff_ms {                                                            // :152
    summary.candidates_skipped_grace += 1;
    continue; // orphan, but younger than grace — hold
}
```

`modified_ms` comes from the object store's `ObjectMeta.last_modified` captured during the LIST
(`:112-113`, `timestamp_millis()` — for `LocalFileSystem`, the filesystem mtime, ms-truncated).
Under the test fixture's `ZERO_GRACE` (`tests/orphan_sweep.rs:24`, `Duration::ZERO`),
`cutoff_ms == now_ms`, so a stray written in the **same millisecond** the sweep reads `now_ms`
satisfies `modified_ms >= cutoff_ms` and is **held instead of deleted**. The test's write and the
sweep's clock read normally land a few ms apart, so the failure is nondeterministic (~1 run in 3
locally when the window collapses); `sweep_keeps_referenced_puffin` (`:363`, asserting
`!stray.exists()` and `objects_deleted == 1` at `:367-372`) trips it most.

**Six tests** pass `ZERO_GRACE` and share the latent race: `sweep_deletes_orphans_keeps_referenced`
(`:123`), `sweep_never_touches_metadata` (`:180`), `sweep_keeps_historical_in_window_file` (`:237`),
`sweep_keeps_dropped_incarnation_file` (`:287`), `sweep_keeps_referenced_puffin` (`:363`),
`sweep_is_idempotent` (`:388,393`). The four that assert a positive deletion count can flake; the
two pure "keeps" tests are insensitive but carry the same boundary.

The module's own doc comment (`:137-139`) claims "a just-written file is deterministically past a
zero grace" — **false today** at the equal-millisecond boundary.

## Decision (operator, 2026-07-21): fix the production boundary

Change the hold condition from `>=` to `>`:

```rust
if modified_ms > cutoff_ms {   // hold only objects STRICTLY younger than the grace age
```

Semantics: an object whose age has reached **exactly** `grace` has satisfied the window and is
sweepable; the sweep holds only objects strictly younger. Consequences:

- `grace == 0` becomes deterministic by construction: a file written before the sweep always has
  `modified_ms <= now_ms == cutoff_ms`, so it is never held — only a *future-dated* mtime
  (clock skew) is held. The doc comment's determinism claim becomes true; reword it to state the
  strict inequality explicitly.
- Production impact is nil: the default grace is **24 h** (`LOOM_ORPHAN_SWEEP_GRACE_SECS`,
  `src/services/runtime/src/lib.rs:287-291`), so a 1 ms shift in a minutes-to-hours-scale
  write-race safety window never binds a real orphan.

The rejected alternative (test-only mtime backdating via `std::fs::File::set_modified`) would keep
the false doc comment, leave the production semantics subtly out of line with its own
documentation, and require touching all six tests instead of one comparison.

## Why no exact-boundary unit test

`now_ms` is read inline inside the sweep (no clock seam exists anywhere in the crate — verified),
so a test cannot construct `modified_ms == cutoff_ms` deterministically from outside. The fix's
correctness at the boundary is by inspection (above); its regression suite is exactly the six
existing `ZERO_GRACE` tests, which the fix converts from flaky to deterministic. Do **not** add a
clock-injection seam for this — that is a larger refactor with no other consumer.

## Testing

- The six existing `ZERO_GRACE` tests are the acceptance suite; run the orphan-sweep test target
  repeatedly (e.g. 20 consecutive runs) to demonstrate the flake is gone. (`buck2 test` has no
  remote test-result cache, so repeated invocations genuinely re-run.)
- `sweep_holds_young_orphan_under_grace` (`:139`, 3600 s grace) pins the hold path and must stay
  green — it is insensitive to the boundary change (its stray is minutes-young against an hour
  grace).
- No new tests required; no SQL changes; no `.sqlx` regen.

## Non-regression

- `candidates_skipped_grace` accounting is unchanged in meaning (count of held candidates).
- No behaviour change for any non-boundary age; no schema, wire, or API change.

## Global constraints (loom-specific, carry into the plan)

- Strict clippy (pedantic + restriction); `buck2 run //tools:prek -- run --all-files` before
  every commit.
- The touched tests are `loom_fixture_test` targets; run them via
  `buck2 test //src/control-plane/postgres:orphan-sweep` (check the exact target name in
  `postgres/BUCK`).

## Acceptance

1. `orphan_sweep.rs` holds only objects strictly younger than the grace age (`>` comparison);
   the module doc comment states the strict-inequality semantics.
2. All six `ZERO_GRACE` tests pass 20 consecutive runs.
3. Existing suites green (`buck2 test //src/...`).

## Interfaces (names the plan consumes)

- Consumes: `sweep_orphans` (`postgres/src/orphan_sweep.rs:100-155`), the six `ZERO_GRACE` tests
  (`postgres/tests/orphan_sweep.rs`).
- Produces: the strict-inequality hold condition + corrected doc comment.
