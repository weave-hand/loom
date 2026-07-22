# Use-case agenda: Grimoire / personal-KG

_A biased wishlist, not the roadmap._

This register is one downstream consumer's opinionated, prioritized list of what it
wants from loom. It is deliberately **single-use-case and biased**: it reflects
Joe's homelab (a D&D campaign manager "Grimoire" and a personal knowledge graph),
not a survey of loom's users. Treat it as input to sequencing in the GitHub
issue tracker (`roadmap`-labeled issues) / (`idea`-labeled issues), not as
committed work. Where an ask already maps to a roadmap issue, it is cited;
net-new asks are flagged `new` and proposed to be filed as labeled GitHub
issues.

Full design rationale lives outside this repo (homelab
`projects/grimoire/loom-mapping.md`).

## Status (reconciled 2026-06-30)

This agenda is **fully accounted for**. A1–A5 shipped. The two remaining asks —
**A6 (atomic per-session check-in)** and **A7 (fine-grained ACL)** — both
**dissolved** during a 2026-06-30 design pass: the Grimoire use case is served by
**composing shipped primitives**, needing **no new loom code**. The chosen model is
**dataset-partition-per-character + coarse per-dataset ACL + versioned
(append-only) datasets + external retry** (see [The chosen model](#the-chosen-model)
below). A7 survives only as a *general* loom deferral for large/dynamic multi-tenant
RLS (`fut-fgac-subject-attribute`); the sole optional nicety is an in-place replace
ingest endpoint (`fut-ingest-overwrite-endpoint`). The vector track (A3) massively
overshot the original ask.

## The use case in one paragraph

Loom is the **governed, durable source of truth** (Iceberg) for a typed
object/link knowledge graph: sourcebook entities and their relationships, text
chunks, embeddings, provenance. The **live application does not read or write loom
on its hot path.** At session start it checks out a working set into a disposable
fast tier (Postgres + pgvector today); during the session all low-latency reads,
mutations, vector search, and fan-out happen there; at session end the delta is
checked back into loom as a new version with lineage. So loom's job is: ingest +
enrich the corpus, hold the canonical graph, version each session, and let a
consumer bulk-read a governed slice in and write a governed delta back out.

## What this use case asks loom to be (and not be)

- **Wants:** governed Iceberg storage of typed objects + links + **vector columns**;
  a **Transform** engine to enrich/connect (extract, resolve, chunk, embed, link);
  **governed mutation** (update/delete, not just insert) for write-back;
  **per-session versioning + lineage** for time-travel; a **fast governed bulk
  read** to hydrate the projection; per-player visibility (met by dataset
  partitioning + coarse ACL — see below, not fine-grained row policy).
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

Status: `done` = shipped; `dissolved` = resolved without new loom work.

### Tier 1 — durable substrate

| id | Ask | Loom status |
| --- | --- | --- |
| A1 | Iceberg overwrite / replace write mode | ✅ **done** `road-iceberg-overwrite-mode` (#152) |
| A2 | Transform output to Iceberg | ✅ **done** `road-iceberg-transform-writes` (#165) |
| A3 | Vector column type + embedding-generation Transform | ✅ **done (column)** `road-vector-column-type` (#168) — and **overshot** (engine-side ANN + `/search`). **A3b** (embedding-generation Transform) untracked — loom stores vectors, an external process embeds |
| A4 | Governed bulk read/export (Arrow Flight) of a slice | ✅ **done** — Flight data plane (#157) + `road-governed-flight-export` (#204) |

### Tier 2 — governed write-back

| id | Ask | Loom status |
| --- | --- | --- |
| A5 | Governed UPDATE / DELETE actions | ✅ **done** `road-update-delete-actions` (#208) |
| A6 | Atomic per-session check-in commit | **dissolved** — versioned/append-only datasets + external retry give cross-dataset consistency without an atomic multi-commit primitive (see below). Retired: `fut-cow-session-checkin` |

### Tier 3 — governance maturity

| id | Ask | Loom status |
| --- | --- | --- |
| A7 | Fine-grained access control (subject-attribute `RowFilter` + entitlement-join / `Exists`) | **dissolved for this consumer** — per-player visibility is served by dataset-partition + coarse per-dataset `Read` grants + hot-tier merge (see below). Kept as a *general*-loom deferral for large/dynamic multi-tenant RLS: `fut-fgac-subject-attribute` |

## The chosen model

The 2026-06-30 design pass worked A7 and A6 to the ground and landed on a model
that needs **no new loom code**. Its shape:

- **Visibility = dataset partitioning, not row policy.** The knowledge graph is
  split into a **global** dataset plus a **per-character** dataset. A player's view
  is `global ∪ their slice`, merged in the **hot projection**. loom governs at the
  **dataset level** with the shipped coarse `Action::Read` grant (grant a subject
  `Read` on `facts_global` + `facts_<player>`, deny the rest). The DM tier is a
  single unfiltered `Read` grant. **No fine-grained `RowFilter` extension is
  needed** — that was A7, now `fut-fgac-subject-attribute` (deferred, general).
  Facts visible to a *subset* of players duplicate across those slices; at campaign
  volume (~thousands of nodes, ~6 profiles) the storage cost is negligible and
  consistency is handled by the versioning below.
- **Check-in = versioned, append-only datasets, no atomic multi-commit.** Each
  session writes its datasets back as new Iceberg snapshots. Because snapshots are
  immutable and append-only, a partial cross-dataset flush **cannot corrupt** (the
  prior consistent state stays fully intact) and an **external retry-until-complete**
  heals any lag. So the "one atomic transaction across all datasets" that A6 implied
  is unnecessary — check-in is external orchestration over shipped per-dataset
  writes. Per-dataset snapshots also give **time-travel to session N** for free
  ("what did the party know then").
- **Checkout** = A4 governed Flight export hydrates the hot tier.
- **Cleanup** = drop-table + the shipped/planned GC (`road-iceberg-gc`,
  `road-iceberg-gc-dropped-table`) reclaim old datasets, or keep them as history.

The one *optional* refinement is `fut-ingest-overwrite-endpoint`: ingest is
append-only today, so a full-state **replace** check-in currently means either
accumulating rows (dedup in the hot tier) or writing a fresh per-session dataset.
An in-place replace endpoint (wiring the shipped `overwrite_parquet_snapshot`) would
make single-stable-name, snapshot-versioned check-in first-class. It is a nicety,
not a requirement.

## Net-new register candidates (reconciled)

- **A3** vector column — ✅ shipped (`road-vector-column-type`, #168).
- **A3b** embedding-generation Transform — still **untracked**; loom stores vectors,
  an external process embeds. Left external (candidate `fut-` entry if wanted).
- **A5** UPDATE/DELETE — ✅ shipped (`road-update-delete-actions`, #208).
- **A6** per-session atomic check-in — **dissolved**; retired as
  `fut-cow-session-checkin` (dropped).
- **A4** governed bulk export — ✅ shipped (`road-governed-flight-export`, #204).
- **A7** fine-grained ACL — **dissolved for this consumer**; kept as the general
  deferral `fut-fgac-subject-attribute`. Optional nicety filed as
  `fut-ingest-overwrite-endpoint`.

## What's left for this consumer

**Nothing loom must build.** The use case is served by composing shipped
primitives (see [The chosen model](#the-chosen-model)). Two optional, deferred
items remain filed as labeled GitHub issues:

- `fut-ingest-overwrite-endpoint` — first-class full-state replace check-in (nicety).
- `fut-fgac-subject-attribute` — general fine-grained ACL, for a *future*
  large/dynamic multi-tenant consumer, not Grimoire.
- `fut-vector-json-serving` / A3b embedding Transform — only if per-object JSON
  vector reads or in-loom embedding generation are ever wanted (both external today).

## Already sufficient (no ask)

- Recursive-CTE graph traversal, FK + join-table (`iss-recursive-cte-iceberg`).
- Single-hop links; atomic snapshot + lineage on the ingest path.
- DataFusion-native Iceberg serving + predicate pushdown.
- Governed mutation (UPDATE/DELETE) and governed columnar export — both shipped.
- Engine-side vector indexes + `/search` — shipped, though this consumer serves
  vectors from its external hot tier and does not rely on them.
- **Per-session check-in** — via dataset-partition + coarse per-dataset `Read`
  grants + versioned (append-only) datasets + external retry; per-player visibility
  and per-session time-travel with no new loom code (see [The chosen model](#the-chosen-model)).
