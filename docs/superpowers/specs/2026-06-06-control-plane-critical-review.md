# Control-Plane Critical Review (as-built, 2026-06-06)

> Written after all five roadmap concerns landed (queue, catalog, ontology, acl,
> lineage). A deliberately critical read of the shipped library along four axes:
> **test gaps, extensibility, coupling, features.** Severity tags: **[H]** high
> (correctness or architecture-blocking), **[M]** medium, **[L]** low/polish.
> Deferred-by-design items already in `docs/FUTURE.md` are referenced, not repeated.

The library is in good shape for its age: clean ports-and-adapters, one
backend-agnostic contract per concern run against both a fake and real Postgres,
hermetic tests, `main` always green. The findings below are where it will bite as
it grows or as the three services start consuming it.

---

## 1. Test gaps

**[H] The `Worker` never heartbeats — long handlers are silently double-executed.**
`Worker::run` is `dequeue → handler → complete/fail` with no `heartbeat` call and no
background lease-renewal. Any handler that runs longer than the queue's
`lock_timeout` has its lock expire mid-flight; another worker reclaims the job and
runs it again. `Queue::heartbeat` exists on the trait and is exercised by
`queue_contract`, but **nothing in the actual worker calls it** — it's dead from the
consumer's perspective. No test runs a handler longer than `lock_timeout`. Fix:
spawn a heartbeat task for the in-flight job (interval < lock_timeout), and add a
worker test with a slow handler asserting single execution.

**[H] No Tx isolation/concurrency contract.** The roadmap's Phase-0 promise was
three Tx properties: commit-visible, rollback-invisible, **and "concurrent
transactions don't observe each other's uncommitted state."** Only the first two
exist (inline in `queue_contract` and `lineage_contract`). The third was never
written. This matters because the two adapters have *genuinely different* isolation
machinery — pg is a real `sqlx::Transaction` (READ COMMITTED); memory is
stage-and-apply-under-a-Mutex. Nobody verifies they agree, and a single-threaded
contract structurally can't. Add a concurrency contract: open Tx A, mutate, assert a
concurrent reader (or Tx B) does not see A's writes until commit.

**[H] Catalog MVCC is only tested on the append path.** `catalog_contract` seeds two
append batches and checks file counts + current snapshot. The MVCC predicate
`begin <= s AND (end IS NULL OR end > s)` — reimplemented independently in both
adapters — is never exercised with a non-null `end`: no dropped/superseded table, no
"query at a snapshot before the table existed," no schema evolution across
snapshots, and `schema_version` is never asserted. The `end`-bounded branch (the
half that makes it MVCC rather than append-only) is untested in the riskiest
duplicated logic in the codebase.

**[M] Queue concurrency (`SKIP LOCKED`) is never tested concurrently.** The whole
point of the pg queue is `FOR UPDATE SKIP LOCKED` fairness; the contract dequeues
single-threaded. No test spawns N workers against M jobs and asserts each job is
claimed exactly once. The memory fake (Mutex + linear scan) and pg (SKIP LOCKED)
could diverge under contention and nobody would know.

**[M] Lineage graph dedup/fan-in is untested.** Only one event produces dataset `C`.
A second event also producing `C` with different inputs would exercise the
union-across-events dedup (`HashSet` in memory, `SELECT DISTINCT` in pg) — untested.
Same for a dataset that is both input and output of one event (one-hop behavior on a
self-edge is unspecified, separate from the deferred transitive cycle-guard).

**[M] Memory `Tx::commit` is not atomic across concerns (and the fake exists to
model exactly this).** `commit` locks `rows`, applies jobs, releases, then locks
`lineage`, applies events — two separate critical sections. A concurrent reader can
observe jobs-applied-but-events-not. The pg adapter gets true all-or-nothing from one
`sqlx::Transaction`. So the fake does **not** faithfully model the atomic guarantee
it's there to let downstream tests rely on. Either apply all staged buffers under one
lock, or document the fake's weaker guarantee (and then the isolation contract above
would catch the divergence).

**[M] Handler panics are uncaught.** `Worker::run` has no `catch_unwind`; a panicking
handler propagates and kills the worker loop, leaving the job locked until expiry. No
test. Decide the policy (treat panic as `Abandon`/`Retry`) and test it.

**[L] Time-based flakiness.** Retry and lock-reclaim assertions use wall-clock
`sleep(lock_timeout + 50ms)`. Fine now, flaky under loaded CI. Consider injectable
clocks if it ever flakes.

**[L] Serde round-trips have one hand-built example each.** `RowFilter` and the
lineage envelope round-trip through a single nested literal. A `proptest`/`arbitrary`
round-trip would cover deep nesting, empty vecs, and unicode names cheaply.

---

## 2. Extensibility

**[H] The `Tx` flat-method seam doesn't scale, and it blocks the headline feature.**
`Tx` hard-codes `enqueue` + `emit`; `core/src/transaction.rs` imports
`queue::NewJob` and `lineage::LineageEvent` directly. Every new transactional op is a
new trait method that every adapter must implement, and `core`'s transaction module
must import that concern's types — so the "cross-concern" seam is really a hand-wired
union of two concerns. More importantly, **catalog has no write op and ontology/acl
writes are autocommit-only**, so the architecture's headline — "snapshot commit +
lineage + enqueue in one transaction" — is unreachable through the current seam. We
built the lineage+queue half; the snapshot leg has no door. (See FUTURE.md, but flag:
this is the gap between the pitch and the build.)

**[H] No pagination/streaming on any read.** `list_types`, `policies_for`,
`events_for`, `snapshots`, `files`, `upstream`/`downstream` all return full `Vec`s.
At real volumes (lineage for a hot dataset, files for a big table) these are
unbounded, and adding pagination later is a breaking signature change to every read
method. Decide a cursor convention now (even if unimplemented) so it's additive.

**[M] `ControlPlane` is nearly useless as a `dyn` trait.** It exposes only `begin()`;
the five concern traits are impl'd directly on the concrete adapter structs with no
accessor methods. So a consumer must depend on the concrete `PgControlPlane` /
`MemoryControlPlane` type, or carry a five-trait generic bound — there's no
`&dyn ControlPlane` that gets you `.lineage()`, `.acl()`, etc. The roadmap's accessor
sketch was dropped; reconsider it (or a single object-safe facade) before three
services each invent their own bound soup.

**[M] Stringly-typed everything blocks the "real type system" evolution.** ontology
`PropertyDef.ty`, acl property names, lineage `DatasetRef` are all bare strings.
Growing toward typed identifiers/logical types (FUTURE.md) touches all three concerns
at once because none share a vocabulary.

**[L] SQL is unchecked at compile time.** Runtime `sqlx::query` everywhere; `query!` /
offline metadata deferred. A column rename breaks at runtime on untested paths. The
contracts are the only guard. Adopting `.sqlx` offline metadata would move these to
compile-time.

---

## 3. Coupling

**[H] Cross-concern references are convention-coupled strings — coupled in reality,
uncoupled in the compiler.** ACL `RowFilter::Compare.property` is "an ontology
property *or* a column, by name"; lineage `DatasetRef` is "a table/type/external
thing, by convention" (`{namespace:"ducklake", name:"main.orders"}`). Nothing links
these to `TypeName` / `TableRef` / `ColumnDef`. Renaming an ontology type silently
invalidates every ACL policy and lineage edge that named it, with zero detection.
This is the most dangerous coupling: the type system says these concerns are
independent; the data says they're joined by hand. Even without full validation, a
shared newtype for "qualified dataset/type identity" + a documented namespacing
convention would make the coupling visible.

**[M] The adapters are single-file monoliths.** `postgres/src/lib.rs` implements all
five concern traits + `Tx` + `pg_insert`/`pg_emit` + codecs in one file; `memory`
likewise. The per-concern SQL is independent but cohabits a ~700-line file sharing
`backend()` and the pool. `core` is already split per concern (`queue.rs`,
`catalog.rs`, …); the adapters should mirror that (`postgres/src/{queue,catalog,
ontology,acl,lineage}.rs`) to shrink the blast radius of a change.

**[M] `MemoryTx` reaches into each concern's private state and grows with every Tx
op.** It now clones `rows`/`notify`/`lineage` and stages two buffers; `commit` is a
sequence of per-concern apply blocks. Each new transactional concern couples `MemoryTx`
to that concern's internal representation. A staged-op abstraction (a `Vec<StagedMutation>`
applied uniformly) would decouple it.

**[L] `core/src/transaction.rs` imports concrete concern types.** Direct consequence
of the flat seam; listed here because it's the literal coupling line: the transaction
module `use`s `queue` and `lineage`. A concern-agnostic staged-op enum would cut it.

---

## 4. Features (missing or shallow)

- **[H] Transactional catalog write / the third atomic leg** — without a catalog
  write op in `Tx`, "snapshot + lineage + enqueue atomic" can't be expressed. Biggest
  pitch-vs-build gap.
- **[H] Transitive lineage** — one-hop only; "where did this ultimately come from" is
  the question lineage exists to answer. (FUTURE.md; needs the cycle-guard.)
- **[M] No deletion / GC / retention anywhere** — ontology can't drop a type, lineage
  is unbounded append, catalog Parquet GC is unbuilt, acl can't delete subjects/roles.
  Long-running deployments grow without bound.
- **[M] No instrumentation** — no `tracing` spans, timings, or structured logs around
  the SQL in a library three services will depend on. First thing ops will want.
- **[M] No batch operations** — enqueue/emit/define are one-at-a-time; bulk lineage or
  job ingest is N round-trips.
- **[L] ACL is minimal** — no explicit-deny/deny-override, no column masking (deny =
  drop), no role hierarchy. Real governance usually needs at least deny-override.
- **[L] Multi-tenancy** — single-tenant; retrofitting is a column + predicate
  everywhere (FUTURE.md).

---

## If you do five things next

1. **Heartbeat in `Worker`** (+ slow-handler test) — silent double-execution is a live
   correctness bug, not a future concern. **[H, small]**
2. **Tx isolation/concurrency contract** — pins the fake↔pg semantic and would catch
   the memory non-atomic-commit divergence. **[H, small]**
3. **Catalog `end`-snapshot (MVCC delete/evolve) contract** — exercises the riskiest
   duplicated logic on its untested half. **[H, small]**
4. **A typed qualified-identity newtype + namespacing convention** shared by ontology
   / acl / lineage, to make the string coupling visible (validation can stay
   deferred). **[H, medium]**
5. **Decide the `Tx` seam's future** before service work: either a concern-agnostic
   staged-op model or accept the flat seam and add the catalog write leg — but stop
   pretending `dyn ControlPlane` is useful. **[H, medium]**

Everything else (adapter file-splitting, pagination convention, `tracing`, `.sqlx`
offline metadata, proptest round-trips) is worthwhile but can trail the five above.
