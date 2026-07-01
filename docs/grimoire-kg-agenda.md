# Use-case agenda: Grimoire / personal-KG

_A biased wishlist, not the roadmap._

This register is one downstream consumer's opinionated, prioritized list of what it
wants from loom. It is deliberately **single-use-case and biased**: it reflects
Joe's homelab (a D&D campaign manager "Grimoire" and a personal knowledge graph),
not a survey of loom's users. Treat it as input to sequencing in
[`ROADMAP.md`](ROADMAP.md) / [`FUTURE.md`](FUTURE.md), not as committed work. Where
an ask already maps to a roadmap id, it is cited; net-new asks are flagged `new`
and proposed for the registers.

Full design rationale lives outside this repo (homelab
`projects/grimoire/loom-mapping.md`).

## Status (reconciled 2026-06-30)

Most of this agenda has shipped. Of the seven asks: **A1–A5 are delivered**, **A6
dissolved** into an external pattern that needs no loom work, and **A7 (fine-grained
access control) is the sole genuinely-outstanding ask** — and it is not yet on any
register. The vector track (A3) massively overshot the original ask. Every row in
the tables below carries its resolution.

## The use case in one paragraph

Loom is the **governed, durable source of truth** (Iceberg) for a typed
object/link knowledge graph: sourcebook entities and their relationships, text
chunks, embeddings, provenance. The **live application does not read or write loom
on its hot path.** At session start it checks out a working set into a disposable
fast tier (Postgres + pgvector today); during the session all low-latency reads,
mutations, vector search, and fan-out happen there; at session end the delta is
checked back into loom as one snapshot with lineage. So loom's job is: ingest +
enrich the corpus, hold the canonical graph, version each session, and let a
consumer bulk-read a governed slice in and commit a governed delta back out.

## What this use case asks loom to be (and not be)

- **Wants:** governed Iceberg storage of typed objects + links + **vector columns**;
  a **Transform** engine to enrich/connect (extract, resolve, chunk, embed, link);
  **governed mutation** (update/delete, not just insert) for write-back;
  **atomic per-session commit + lineage** for time-travel; a **fast governed bulk
  read** to hydrate the projection; eventually **fine-grained (per-subject) read
  policy** if consumers read loom directly.
- **Explicitly does NOT want loom to be:** an **ANN engine** (vector serving stays
  in the external hot tier), a **graph engine** (1-3 hop traversal is recursive
  SQL, which loom already serves), or a **live OLTP / fan-out tier** (that is the
  disposable projection).
  - **Note (2026-06-30):** loom has since shipped engine-side ANN anyway — Puffin
    exact + IVF-Flat + HNSW indexes and an external `/search` kNN endpoint
    (`road-puffin-vector-index` / `road-ivf-vector-index` / `road-hnsw-vector-index`
    / `road-vector-search-endpoint`) — driven by needs beyond this consumer. This
    consumer still does **not need** it (it serves vectors from the external hot
    tier), so the non-goal holds *as a preference*; the capability simply now
    exists should that ever change. The graph-engine and OLTP non-goals still hold.

## The asks (prioritized)

Tiers are dependency-ordered. Status: `done` = shipped; `planned` = existing
roadmap id, not yet built; `dissolved` = resolved without loom work; `outstanding`
= wanted, not on any register.

### Tier 1 — durable substrate

| id | Ask | Why | Loom status |
| --- | --- | --- | --- |
| A1 | Iceberg overwrite / replace write mode | any mutable state (entity upserts on write-back, transform replace) | ✅ **done** `road-iceberg-overwrite-mode` (#152) |
| A2 | Transform output to Iceberg | the enrich/connect DAG substrate | ✅ **done** `road-iceberg-transform-writes` (#165), polymorphic `Tx` |
| A3 | Vector column type (`FixedSizeList<f32,N>`) + embedding-generation Transform | embeddings as governed, lineage-tracked, reproducible data | ✅ **done (column)** `road-vector-column-type` (#168) — and **overshot**: engine-side ANN + `/search` also shipped. **A3b** (embedding-generation Transform) is `outstanding`/untracked — loom stores vectors, an external process embeds |
| A4 | Governed bulk read/export (Arrow Flight) of a slice | hydrate the hot-tier projection fast, columnar | ✅ **done** — Flight data plane (#157) + governed export `road-governed-flight-export` (#204) |

### Tier 2 — governed write-back

| id | Ask | Why | Loom status |
| --- | --- | --- | --- |
| A5 | Governed UPDATE / DELETE actions | write-back upserts mutable state; insert-only is not enough | ✅ **done** `road-update-delete-actions` (#208) |
| A6 | Atomic per-session check-in commit | one snapshot + lineage per session = time-travel to past graph state | **dissolved** — resolved as an external pattern (see below); enabler is `road-cow-inline-shadow` (planned) |

### Tier 3 — governance maturity (only if consumers read loom directly / multi-tenant)

| id | Ask | Why | Loom status |
| --- | --- | --- | --- |
| A7 | Fine-grained access control: `RowFilter` gains subject-attribute references and an entitlement-join / `Exists` leaf | per-subject row access (the consumer's per-player grants; multi-tenancy; any RLS). The query API already holds the subject. | **outstanding** — not on any register. ACL is P4, literal-only; the governed-SQL path (`road-external-sql-governed-catalog`) enforces per-type literal `RowFilter`s only. The subject-attribute / entitlement-join extension is unbuilt and unscheduled |

### A6 resolution (why it dissolved)

The "session" is entirely the **consumer's** concept, in the **external** hot tier;
loom never needs to know sessions exist. Stripping the session framing away, the
only thing a client cannot do from outside is an **atomic multi-write commit
boundary** (N writes → one snapshot + one lineage event). That property is YAGNI
here: because the consumer already tracks its own delta and loom's write actions
return per-item status, a **retry-until-complete** check-in (resend only the writes
that failed, with their correct insert/update/delete classification) achieves
eventual completeness without a loom-side transaction. So check-in stays external
(loop the existing governed write actions + retry). Its real *performance* enabler
is `road-cow-inline-shadow` (planned), which makes each per-object mutation
O(change) instead of a whole-table rewrite. **Idempotent upsert** (`fut-cow-identity-change`,
deferred) would harden the one remaining edge (a lost ack on an insert), but is a
convenience, not a requirement. `fut-cow-session-checkin` should be retired as
resolved-external.

## Net-new register candidates

Status of the original `new` proposals, reconciled:

- **A3** vector column — ✅ shipped (`road-vector-column-type`, #168).
- **A3b** embedding-generation Transform — still **untracked**; loom stores vectors,
  an external process embeds. Propose a `fut-` entry (or leave external) —
  currently only mentioned in prose on `road-vector-column-type`.
- **A5** UPDATE/DELETE — ✅ shipped (`road-update-delete-actions`, #208).
- **A6** per-session atomic check-in — **dissolved** (external; see A6 resolution).
  Retire `fut-cow-session-checkin`.
- **A4** governed bulk export — ✅ shipped (`road-governed-flight-export`, #204).
- **A7** attribute/entitlement-aware `RowFilter` (FGAC) — **the one genuinely-open
  ask.** Not on any register. This is the most general: table-stakes for
  "governance follows the data" beyond static markings, useful far past this one
  consumer. Candidate for promotion to `FUTURE.md`/`ROADMAP.md`.

## What's left for this consumer

1. **A7 — fine-grained (subject-attribute) access control.** The sole outstanding
   ask; deferrable past v1 for *this* consumer (the hot tier enforces per-player
   grants during a session), but high-value to loom in general. Needs a register
   item before it is buildable.
2. **A3b — embedding-generation Transform.** Optional; the use case embeds
   externally today, so this only matters if embedding-as-governed-Transform is
   wanted. Untracked.
3. **`road-cow-inline-shadow`** (already planned, spec on disk, unclaimed) — the
   real enabler for a cheap external check-in loop. Not strictly an agenda ask, but
   the highest-leverage planned item for this use case.

## Already sufficient (no ask)

- Recursive-CTE graph traversal, FK + join-table (`iss-recursive-cte-iceberg`).
- Single-hop links; atomic snapshot + lineage on the ingest path.
- DataFusion-native Iceberg serving + predicate pushdown.
- Governed mutation (UPDATE/DELETE) and governed columnar export — both shipped.
- Engine-side vector indexes + `/search` — shipped, though this consumer serves
  vectors from its external hot tier and does not rely on them.
