# Decision: the `Tx` seam stays flat (Step 2a #5)

> **Status:** decided (2026-06-07). Fifth and final Step 2a hardening item from
> `2026-06-06-control-plane-critical-review.md` (§2). This is a **decision record**, not
> an implementation — the resolution is "keep what we have, defer the rest, don't
> gold-plate." No code change beyond the docs that point here.

## Context

The review flagged the cross-concern `Tx` seam on three counts:
1. It's a flat hand-wired union — `Tx { commit, rollback, enqueue, emit }`, with
   `core/transaction.rs` importing `queue::NewJob` and `lineage::LineageEvent` directly —
   that "doesn't scale" as concerns join.
2. It blocks the headline "snapshot + lineage + enqueue atomic" feature, because the
   **catalog has no write op** (it's read-only; the DuckLake client owns `ducklake.*`).
3. `dyn ControlPlane` exposes only `begin()`, so it's nearly useless for dynamic dispatch
   over concerns.

The task was to *decide the seam's future before Step 3 service work*, not necessarily to
refactor.

## Decision

**A. Keep the flat `Tx` seam.** Transactional ops stay flat methods on `Tx`; a new one is
added as a method when a concern genuinely needs transactional writes. We do **not** adopt
per-concern sub-handles (`tx.lineage().emit(…)`) or a concern-agnostic staged-op model now.

Rationale: there are exactly **two** transactional ops today (`enqueue`, `emit`) and a
plausible third (the catalog write, below) that is itself deferred. Building a composition
abstraction for 2–3 ops is speculative — the [[opinionated-not-pluggable]] grain. The minor
`core/transaction.rs` ↔ concern coupling (two `use` lines) is acceptable. The flat-vs-
aggregator question is genuinely *re-openable* the moment a third transactional concern
lands and proves flat insufficient; until then, flat is the smaller, working choice.

**B. Defer the catalog write leg to the ingest worker (Step 3).** The third atomic leg —
committing a DuckLake snapshot inside the same `Tx` as the lineage emit + downstream
enqueue — is **entirely an ingest-worker concern**. *How* loom commits a snapshot (write
`ducklake.*` directly vs. drive the DuckLake client vs. register a loom-produced snapshot)
is inseparable from the ingest service's design and the still-open multi-writer /
DuckLake-concurrency question. It cannot be responsibly designed in `core` ahead of that
service, so it moves into **Step 3 (ingest)**. When ingest defines it, it joins the seam as
one more flat method (or triggers the (A) re-evaluation).

**C. Leave `dyn ControlPlane` simple.** We do **not** add per-concern accessors
(`fn lineage(&self) -> &dyn Lineage`). Consumers hold the concrete adapter
(`PgControlPlane`/`MemoryControlPlane`) or are generic over the concern traits they need —
no consumer requires `&dyn ControlPlane` today. Adding accessors for a hypothetical
dynamic-dispatch consumer is speculative; we assess the need when a real one appears.

## Consequences

- Step 3 is unblocked: the seam's direction is settled (flat), and the one piece that
  touches it (the catalog write) is explicitly owned by the ingest worker.
- `core/transaction.rs` keeps its two concern imports; `Tx` keeps its four methods.
- `docs/FUTURE.md` is updated so the "Wider `Tx` composition" and "transactional catalog
  write" notes read as **decided/assigned** rather than open questions.
- Re-evaluation trigger: if a *fourth* concern (beyond queue, lineage, and the ingest
  catalog write) needs transactional writes, revisit (A) — sub-handles or staged-op may
  then earn their keep.

## Non-goals

- No `Tx` refactor, no sub-handle/staged-op machinery, no `dyn ControlPlane` accessors.
- The catalog write op itself — designed with the ingest worker in Step 3, not here.
