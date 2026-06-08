# Design: tracing instrumentation (Step 2b, group 3)

> **Status:** approved design. Step 2b hardening group 3 from
> `2026-06-06-control-plane-critical-review.md`. Adds `tracing`-facade
> instrumentation to the worker and the two adapters. **Facade only** — no
> subscriber, no exporter, no `metrics` crate; the Step 3 services install the
> subscriber that gives these spans somewhere to go.

## Why facade-only

`memory`, `postgres`, and `worker` are libraries — no `main`, no runtime, no
subscriber. The idiomatic Rust pattern is that libraries emit spans/events through
the `tracing` facade and the *binary* installs a `Subscriber`. The binaries are
Step 3 (services). So this group wires the emit side everywhere it's cheap and
useful now; when the services land they get observability for free by installing one
subscriber. A `metrics` registry/exporter without a process to scrape it would emit
into a no-op recorder, so it's deferred to Step 3 (YAGNI).

## Dependencies (added via the reindeer workflow)

| Crate | Dep | Kind | Why |
|-------|-----|------|-----|
| `worker`, `memory`, `postgres` | `tracing` | normal | the emit facade (`#[instrument]`, `info!`/`warn!`/`debug!`) |
| `worker` | `tracing-test` | dev | capturing subscriber for the one assertion test; handles the "global default set once" footgun and cross-thread capture so the test isn't flaky |

Workflow for each (see CLAUDE.md "Third-party Rust deps"): add to the crate's
`Cargo.toml` → `buck2 run //tools:reindeer -- update` (lockfile) → `./tools/buckify.sh`
→ add `//third-party:tracing` (and `//third-party:tracing-test` to the worker's
`rust_test` deps) to the crate `BUCK`. All steps or `reindeer-check` fails.

`tracing` default features are fine (we use `std` + the macros). `tracing-test` is a
dev-dep, so it never ships in the library.

## What gets instrumented (selective — mutations + lifecycle, not reads)

### worker (`src/control-plane/worker/src/lib.rs`)
- A **per-job span** wrapping handler processing:
  `#[instrument(skip(self, handler), fields(job_id = %id, kind = %job.kind))]`
  — applied to the section that runs one job (extract a small `async fn process_one`
  if cleaner, or `instrument` the inline block via `tracing::info_span!(...).in_scope`
  / `.instrument()`; implementer's choice as long as the span carries `job_id` + `kind`).
- **Events on the existing match arms** (no logic change — just add a line):
  - dequeued a job → `debug!("dequeued")`
  - `Ok(Ok(()))` → `info!("job completed")` then `complete`
  - `Ok(Err(JobFailure { error, policy }))` → `warn!(error = %error, ?policy, "job failed")` then `fail`
  - `Err(panic)` → `warn!(panic = %msg, "handler panic contained")` then `fail(... Abandon)`

### adapters (both `memory` and `postgres`, mirrored)
`#[instrument(skip(self), level = "debug")]` on these mutating methods:

- **queue.rs:** `enqueue`, `dequeue`, `complete`, `fail`, `heartbeat`
- **ontology.rs:** `define_type`, `define_link`
- **acl.rs:** `define_subject`, `define_role`, `assign_role`, `unassign_role`,
  `grant`, `revoke`, `set_policy` (**skip the `policy` arg**), `clear_policy`
- **lineage.rs:** `emit` — **skip the `event` payload**; record identifying fields:
  `#[instrument(skip(self, event), fields(run_id = ?event.run_id, event_type = ?event.event_type), level = "debug")]`
- **transaction.rs:** `commit`, `rollback`, `enqueue`, `emit` (same skip rules as above
  for `emit`)

### NOT instrumented (pure reads — keep span noise down)
`get_type`, `list_types`, `links`, `resolve`, `current_snapshot`, `snapshots`,
`files`, `schema`, `check`, `policies_for`, `upstream`, `downstream`, `events_for`.

## Conventions

- **Always `skip(self)`.** Adapters hold a pool/locks; never format them.
- **Skip large/opaque args; record their identity instead.** `event` payload, `policy`
  → skipped, with `run_id`/`event_type` (lineage) recorded as fields. `grant`/`revoke`
  args are small enums/ids — fine to let them be captured (or skip + record, impl's
  choice; keep it readable).
- **Levels:** worker per-job span and `info!("job completed")` at `info`; failures and
  panics at `warn`; all adapter mutations at `debug`. Default-quiet: a service that
  wants the adapter chatter sets `RUST_LOG`/its filter to `debug`.
- **No logic changes.** Instrumentation is attributes + event lines only. A method's
  return value, error handling, and control flow stay byte-identical.

### async-trait + `#[instrument]` gotcha
The trait methods are `#[async_trait]` (desugars to `Box::pin(async move {…})`).
`#[instrument]` on the method works with modern `tracing`, but the attribute ordering
matters: place `#[instrument(...)]` **directly on the method**, below nothing else
(async-trait rewrites the body and `instrument` wraps the resulting future). The test
below is what proves it actually fires rather than silently compiling to nothing.

## Test (one, in the worker crate)

`src/control-plane/worker/tests/worker.rs` gains a `#[traced_test]` (from
`tracing-test`) test that:

1. Builds a `MemoryControlPlane`, enqueues one `"boom"` (panicking) job.
2. Drives the worker so the instrumented code runs **on a thread the capturing
   subscriber observes** — i.e. await `worker.run(token)` with a token cancelled after
   the job drains, on the test's runtime (reuse the existing
   `handler_panic_is_contained` driving pattern; `tracing-test` captures globally so
   spawned tasks are fine too).
3. Asserts the instrumentation fired: `assert!(logs_contain("handler panic contained"))`
   (the panic event) — and, if `tracing-test` surfaces span fields, that the per-job
   span carried the `job_id`. The minimum bar is the panic event assertion; it proves
   the worker's `tracing` wiring is live end-to-end.

If `tracing-test`'s cross-thread capture proves flaky for spawned tasks, fall back to
driving `run()` inline (awaited on the test thread) rather than `tokio::spawn` — the
event then fires on the captured thread deterministically.

No new tests for the adapters: their instrumentation is the same `#[instrument]`
mechanism proven by the worker test, and asserting `debug` spans across every adapter
method would be high-noise, low-value. The existing contract suites already prove the
methods' behaviour is unchanged.

## Verification

- `env -u BUCK_PREFER_REMOTE buck2 test --local-only //src/...` → all green; worker
  test count goes 5 → 6; every other count unchanged (behaviour preserved).
- `tools/clippy-all.sh` clean (watch for unused `tracing` imports if a file ends up
  with no events — only `use tracing::...` what's used).
- `reindeer-check` in-sync (the dep workflow ran fully).
- `prek run --all-files` green.

## Scope / non-goals

- **No `metrics` crate, no counters/histograms.** Deferred to Step 3.
- **No subscriber/exporter/fmt setup** anywhere — libraries only emit.
- **No spans on read methods.**
- **No new public API.** `#[instrument]` and event macros are internal; the trait
  signatures are untouched, so `core`/`testkit` and all dependents are unaffected.
- One PR ("tracing instrumentation"); commit the dep-add + buckify separately from the
  instrumentation if it keeps the diff readable, implementer's discretion.
