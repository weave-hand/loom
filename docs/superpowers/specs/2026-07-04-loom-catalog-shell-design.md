# Loom Catalog Shell — Design

**Status:** design approved (2026-07-04)
**Audience:** implementer with no prior context on `src/ui/`.
**Source design:** `~/Downloads/design_handoff_loom_catalog/` (`README.md` + the two
`.dc.html` prototypes). Treat the handoff README's **Design Tokens** section as the
authoritative style contract; the prototype HTML is reference, not code to port.

## Goal

Turn the design handoff's five-surface **master-detail application shell** into the
existing Yew UI (`src/ui/`), lighting up the two surfaces the backend can actually
serve (**Catalog**, **Ontology**) and rendering the other three
(**Pipelines**, **Workbooks**, **Dashboards**) as honest "not available on this
instance" placeholders. Add the two small backend routes the Catalog surface needs.

## Backend reality (what exists today)

Registered query-api / runtime routes relevant here:

| Design need | Route today | Verdict |
|---|---|---|
| Catalog list | `GET /datasets` → `{datasets:[{schema,name}]}` | thin — no project/updated/rows |
| Catalog › Schema | `GET /datasets/:schema/:table` → `{table,snapshot_id,snapshot_time,columns:[{name,ty,nullable}]}` | ✅ |
| Catalog › Preview | — | ❌ missing route |
| Catalog › Lineage | `GET /lineage/datasets/:ns/:name/upstream` + `/downstream` → closure | ✅ |
| Catalog › History | `GET /lineage/runs/:run_id/events` (per-run only) | ⚠️ no per-dataset run list |
| Ontology types | `GET /ontology/types`, `GET /ontology/types/:name` | ✅ |
| Ontology objects | `GET /objects/:type` (keyset paged), `/objects/:from/links/:link` | ✅ |
| Pipelines / Workbooks / Dashboards | — | ❌ no backing concept |

## Scope decisions (locked)

1. **Surfaces:** build the full 5-surface shell. Catalog + Ontology are live; the
   other three are stubbed placeholders with a real nav entry and the correct
   per-surface accent.
2. **Backend gaps to close now:** (a) enrich `GET /datasets`; (b) add a dataset
   **preview** route. **Not** now: a per-dataset runs/history route (History tab is
   stubbed honestly).
3. **Shell composition:** a presentational `Shell` (chrome only) + concrete
   per-surface components. **No** per-surface config object / generic
   `Shell<Surface>` (opinionated-not-pluggable).
4. **Routing:** in-memory surface + per-surface selected-row/active-tab state. No
   `yew-router` yet.
5. **Lineage:** the **drawer** Lineage tab is real (SVG mini-DAG from upstream +
   downstream closures). The standalone **full-canvas** lineage view (wireframe 2a)
   is **stubbed** — a placeholder page reachable from the drawer's "Open full
   view ↗" affordance.

## Backend design (Stage A)

### A1 — Enrich `GET /datasets`

Each dataset entry gains `project` (= its schema/namespace) and `updated` (= the
table's current snapshot time, RFC3339). Implementation: after `list_tables`, look
up `current_snapshot` per table for its `time`; `project` is `schema`. `rows` is not
returned (no cheap count). Auth is coarse (authenticated), unchanged from today's
`list_datasets` (it takes `_subject` but does not per-row ACL-filter).

```
GET /datasets
→ { "datasets": [ { "schema": "main", "name": "txns",
                    "project": "main", "updated": "2026-07-01T12:00:00Z" }, … ] }
```

Note the per-table snapshot lookup is N calls for N tables — acceptable for the MVP
list sizes; flag as a potential N+1 to revisit if dataset counts grow.

### A2 — New `GET /datasets/:schema/:table/preview?limit=N`

query-api builds `SELECT * FROM "schema"."table" LIMIT N` and runs it over the
**existing** internal Flight SQL path to the engine (the same `CommandStatementQuery`
machinery `/objects` uses — inline the limit, send SQL, decode the Arrow IPC result).
Returns column names + row cells as strings plus a `sampled` flag.

```
GET /datasets/main/txns/preview?limit=20
→ { "columns": ["id","merchant","amount","ts"],
    "rows": [ ["1","ACME","12.40","2026-…"], … ],
    "sampled": true }
```

- `limit` default 20, capped (e.g. 200). Malformed limit → 400.
- Unknown table → 404 (map the engine/catalog error like `get_dataset` does).
- Auth coarse, mirroring `list_datasets`. (Fine-grained dataset ACL on preview is a
  deferred follow-up — record in `docs/FUTURE.md`.)
- Testable via the query-api `e2e-support` harness (seed a landed dataset, GET the
  preview, assert columns/rows/sampled). Any pure response-shaping helper lives in a
  unit-testable function.

## Frontend design (Stages B–E)

All new UI logic that can be tested without a DOM goes in the lint-clean
`loom_ui_core` lib with `rust_test` targets. Component *rendering* is not unit-testable
(buck2's runner has no DOM — see `src/ui/CLAUDE.md`); it is verified by eye in the
gallery and, where valuable, the fantoccini login-e2e pattern.

### File layout (`src/ui/src/`)

- `shell.rs` — `Shell` presentational chrome: app bar (logo + wordmark + surface
  switcher + search-slot + avatar), a main-list region (children), and a drawer
  region (optional children). Sets `--loom-accent` from the active surface on its
  root so the token layer re-themes. Emits `on_surface_switch(Surface)`.
- `surfaces/mod.rs` — `Surface` enum (`Catalog | Pipelines | Ontology | Workbooks |
  Dashboards`) with `accent()`, `label()`, `is_live()`; re-exports the views.
- `surfaces/catalog.rs` — `CatalogView`.
- `surfaces/ontology.rs` — `OntologyView` (today's Explorer, re-homed).
- `surfaces/stub.rs` — `StubView` (coming-soon empty state) + `LineageFullStub`.
- `net.rs` — add `fetch_datasets`, `fetch_dataset_detail`, `fetch_preview`,
  `fetch_lineage(dir)` (reuse the bearer-auth + 401-fails-closed pattern already in
  `net.rs`).
- `loom_ui_core` — pure parsers (`parse_datasets`, `parse_preview`,
  `parse_lineage_closure`) + a **DAG builder** (`lineage_dag(upstream, downstream,
  self) -> {nodes, edges}` with stage/kind classification), all `rust_test`'d.

`main.rs`'s `App`: hold `active_surface` + a per-surface `(selected_row, active_tab)`
map in state; render `Shell` with the active surface's view as children.

### TopNav

Extend `TopNav` so a nav item can be marked active and render the 2px accent
underline offset below (per README app-bar spec). Inactive items muted; clicking
emits the surface switch.

### OntologyView (Stage C)

Ports the current `Explorer` behavior into the Shell, unchanged in function:
type list → paginated objects `DataTable` (keep "Load more") → drawer with
**Properties** + **Links** tabs sourced from `GET /ontology/types/:name`. Purple
accent (`#8b5cf6`). After this stage, today's Explorer capability is preserved,
restyled.

### CatalogView (Stages D–E)

Blue accent (`#3b82f6`). List columns Name · Project · Rows · Updated from the
enriched `/datasets` (Rows shows `—`). Filter-chip row + "Sort ▾" are presentational
for the MVP (no server sort). Selecting a row drives the drawer:

- **Schema** (D) — column list `name type` (name in link-blue), nullable annotation,
  from `GET /datasets/:schema/:table`.
- **Preview** (D) — sampled table from the new preview route; footnote
  "Showing N of … · sampled".
- **Lineage** (E) — SVG mini-DAG on the dotted-grid canvas built from
  `upstream` + `downstream` closures via `loom_ui_core::lineage_dag`; the current
  dataset is the accent-glow center node. Caption "N upstream · M downstream". Carries
  an **"Open full view ↗"** button → `LineageFullStub`.
- **History** — honest stub: "Per-dataset run history isn't available on this
  instance yet." (no per-dataset runs route).

### Stubs

- `StubView` — for Pipelines/Workbooks/Dashboards: the shell renders normally, the
  list/drawer region shows a centered "This surface isn't available on this instance
  yet" empty state in the surface's accent.
- `LineageFullStub` — the deferred full-canvas lineage view: same coming-soon
  treatment, reached from the Catalog drawer Lineage tab.

## Staging (each independently shippable + testable)

- **A** — backend: enrich `/datasets` (A1) + preview route (A2). Verified by query-api
  e2e + unit tests. No UI change.
- **B** — Shell chrome + per-surface accent + surface switcher + `StubView`. All five
  nav entries switch; three show stubs; Catalog/Ontology regions empty-wired.
- **C** — `OntologyView`: Explorer ported into the Shell, parity with today.
- **D** — `CatalogView` list + Schema + Preview drawer tabs.
- **E** — Catalog Lineage mini-DAG (`lineage_dag` builder + SVG render) +
  `LineageFullStub`.

## Deferred (record in `docs/FUTURE.md` as items land)

- Real routing (`/catalog/:id`, per-surface URL state).
- Dataset **row counts** in the list.
- **Full-canvas lineage view** (wireframe 2a) — stubbed now, built later.
- Per-dataset **runs/history** route → real History tab.
- Fine-grained **dataset ACL** on preview (currently coarse).
- Pipelines / Workbooks / Dashboards backends (and thus real surfaces).
- Schema **PII / null-%** annotations (design shows them; no backend source).
- Server-side Catalog **sort/filter** (chips are presentational in the MVP).

## Testing strategy

- **Backend:** query-api `e2e-support` tests for A1 (enriched fields present) and A2
  (columns/rows/sampled, bad-limit 400, unknown-table 404); pure shaping helpers
  unit-tested.
- **UI pure logic:** `loom_ui_core` `rust_test`s for every parser and the
  `lineage_dag` builder (node/edge/stage classification on representative closures).
- **UI rendering:** verified in the gallery by eye; no inline `#[test]` for components
  (the `no-inline-tests` hook forbids it and there's no DOM in the runner).
