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

## The asks (prioritized)

Tiers are dependency-ordered. Status: `planned` = existing roadmap id; `new` =
proposed.

### Tier 1 — durable substrate

| id | Ask | Why | Loom status |
| --- | --- | --- | --- |
| A1 | Iceberg overwrite / replace write mode | any mutable state (entity upserts on write-back, transform replace) | planned `road-iceberg-overwrite-mode` |
| A2 | Transform output to Iceberg | the enrich/connect DAG substrate | planned `road-iceberg-transform-writes` |
| A3 | Vector column type (`FixedSizeList<f32,N>`) + embedding-generation Transform | embeddings as governed, lineage-tracked, reproducible data | `new` |
| A4 | Governed bulk read/export (Arrow Flight) of a slice | hydrate the hot-tier projection fast, columnar | data plane planned `road-engine-wire-flight`; governed export `new` on top |

### Tier 2 — governed write-back

| id | Ask | Why | Loom status |
| --- | --- | --- | --- |
| A5 | Governed UPDATE / DELETE actions | write-back upserts mutable state; insert-only is not enough | `road-iceberg-actionengine` is insert-only; UPDATE/DELETE `new` |
| A6 | Atomic per-session check-in commit | one snapshot + lineage per session = time-travel to past graph state | `new` (composes existing snapshot+lineage with A1 + append) |

### Tier 3 — governance maturity (only if consumers read loom directly / multi-tenant)

| id | Ask | Why | Loom status |
| --- | --- | --- | --- |
| A7 | Fine-grained access control: `RowFilter` gains subject-attribute references and an entitlement-join / `Exists` leaf | per-subject row access (the consumer's per-player grants; multi-tenancy; any RLS). The query API already holds the subject. | `new` (acl is P4, literal-only) |

## Net-new register candidates

These have no roadmap id yet and are the concrete proposals from this use case:

- **A3** vector column + embedding Transform (rides A2).
- **A5** UPDATE/DELETE action semantics (rides A1).
- **A6** per-session atomic check-in (orchestration over existing primitives).
- **A7** attribute/entitlement-aware `RowFilter` (FGAC). This is the most general:
  table-stakes for "governance follows the data" beyond static markings, useful
  far past this one consumer.
- **A4** governed bulk export over the engine-wire data plane.

## Biased build order

`A1` first, it unblocks both A2->A3 (enrich + embeddings) and A5->A6 (write-back),
and is already planned. Then the two tracks run in parallel. `A4` follows the
engine wire. `A7` is independent and deferrable past v1 for this consumer (the hot
tier enforces per-player grants during a session); it is listed last here only
because of that, not because it is low-value to loom in general, it is the opposite.

## Already sufficient (no ask)

- Recursive-CTE graph traversal, FK + join-table (`iss-recursive-cte-iceberg`).
- Single-hop links; atomic snapshot + lineage on the ingest path.
- DataFusion-native Iceberg serving + predicate pushdown.
