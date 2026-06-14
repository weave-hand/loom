# Design: control-plane adapter tracing pass + 2b closeout (Step 2b)

> **Status:** approved design (2026-06-14). A small observability + bookkeeping slice that
> finishes Step 2b. During scoping, the rest of the 2b "trailing hardening" checklist turned out
> already delivered (adapter split, the `SKIP LOCKED` concurrency contract, the worker
> handler-panic policy, pagination, proptest round-trips, `.sqlx`, the `Conflict` variant) — so
> the only genuine remaining work is **consistent `tracing` instrumentation** across the adapter
> concerns that lag, plus **correcting the stale roadmap** to reflect what actually shipped.

## Goal

Make adapter SQL methods observable uniformly: every `ControlPlane` trait-impl method on both
adapters carries a `#[tracing::instrument]` span, matching the convention already applied to the
`acl` and `queue` concerns. Today `catalog`, `snapshot`, and `lineage` lag (zero or partial
instrumentation), so traces drop a span when execution crosses those concerns. And bring the
roadmap's Step 2b section back in sync with reality.

## Scope

### (A) Tracing pass

Add `#[tracing::instrument(skip(self), level = "debug")]` to each **trait-impl method** that
lacks it in:

- `src/control-plane/postgres/src/catalog.rs` (5 methods, 0 instrumented)
- `src/control-plane/postgres/src/snapshot.rs` (9 methods, 0 instrumented)
- `src/control-plane/postgres/src/lineage.rs` (~6 remaining; 1 already instrumented)
- `src/control-plane/memory/src/catalog.rs` (4 methods, 0 instrumented)
- `src/control-plane/memory/src/lineage.rs` (~3 remaining; 1 already instrumented)

(The memory adapter has no separate `snapshot.rs` — its snapshot ops live in `catalog`/
`transaction`, which are covered here / already instrumented.)

**Convention.** Match `acl`/`queue` exactly: `#[tracing::instrument(skip(self), level =
"debug")]` on the method. Beyond `self`, additionally `skip(...)` any **large or verbose
argument** (e.g. a `Vec<DataFile>` of appended files, a JSON payload, a column-spec list) so a
debug span doesn't log a whole batch — instrument these by recording a cheap summary field
(e.g. `files = files.len()`) via `fields(...)` rather than dumping the value, or simply
`skip` it. Only instrument **trait methods**, not private helpers, unless a helper is the
natural span boundary (judgment per method; default is trait methods only).

**No behavior change.** This is pure instrumentation; the spans are emitted at `debug` and have
no effect unless a subscriber is installed.

### (B) Error-variant audit (no code change)

Record the conclusion reached during scoping: `ControlPlaneError::Conflict` is **wired**
(constructed in both ACL adapters on a uniqueness race, asserted in the contract);
`ControlPlaneError::Unauthorized` is **retained as a documented reservation** for the Step-3
service auth layer (its doc comment already states this). The roadmap's "wire or drop the unused
`Conflict`/`Unauthorized` variants" item resolves to **wired / reserved** — no code change.

### (C) Roadmap correction

Update `docs/superpowers/specs/2026-06-06-loom-roadmap.md`:

- In the **Step 2b — Trailing hardening** checklist, mark the delivered items: the per-concern
  adapter split (`postgres`/`memory` are already split), the `SKIP LOCKED` concurrency test
  (`queue_concurrency_contract`, both adapters), the worker handler-panic policy
  (`catch_unwind` → `fail(.., Abandon)` + test), pagination, proptest round-trips, `.sqlx`, and
  the `Conflict`/`Unauthorized` resolution from (B). Note this tracing pass as the last item,
  delivered.
- In **"Where we are,"** state that Step 2b is now effectively closed (this slice finishes it),
  so the active track is the next capability — the queue-driven **Transform worker** (the one
  remaining unbuilt service pillar) and/or continuing richer reads (derived properties /
  multi-hop).

## What this slice is NOT

- No new runtime behavior, no new public API, no new test logic (the concurrency contract and
  panic test already exist and stay as-is).
- No metrics backend / subscriber wiring — that is a service-runtime concern, separate.
- No restructuring of the adapter files (already split per concern).

## Testing

- The full first-party build + test sweep stays green (`buck2 build //src/...` + `buck2 test
  //src/...`), proving the added attributes compile and change no behavior.
- **`clippy` clean** — `#[tracing::instrument]` can surface lints (e.g. an unused `fields`
  binding); the pass must leave `./tools/clippy-all.sh` at exit 0.
- No new test targets. (The instrumentation has no observable behavior to assert without a
  subscriber; the existing contract suite is the regression guard.)

## File structure

- Modify: `src/control-plane/postgres/src/{catalog,snapshot,lineage}.rs`
- Modify: `src/control-plane/memory/src/{catalog,lineage}.rs`
- Modify: `docs/superpowers/specs/2026-06-06-loom-roadmap.md`

## Roadmap

Closes the last open item of **Step 2 — Harden the control plane → 2b**. After this, Step 2 is
complete and Step 3 (services) is the sole active track.
