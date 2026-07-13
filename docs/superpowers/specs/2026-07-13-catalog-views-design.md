# Catalog views — dataset/type decoupling Design

> **Status:** design (direction). Registered as `#road-catalog-views`
> (ROADMAP, area:catalog). A separate work agent writes the implementation
> plan from this spec and builds it; the slice may decompose at plan time
> (catalog concern → engine expansion → write-through → routes/lineage).

## Problem

A subset of a dataset cannot carry its own permissions. The coupling is
structural:

- `ObjectType.table: TableRef` (`core/src/ontology.rs:39-54`) binds a type
  to exactly one whole physical table; `Ontology::resolve`
  (`ontology.rs:928`) returns that one table. A type may already declare a
  **column** subset (`ontology.rs:850-858`, `bind.rs:68` — "a type is a
  view over the table") but never a **row** subset.
- `PolicyTarget` is `{Type, Table}` (`core/src/acl.rs:68-72`) — grants
  target a whole type or a whole table. Row filters / column masks exist
  (`Policy`, `acl.rs:202-212`) but only as policies bolted onto a
  whole-Type/Table grant, literal-only.

The shipped workaround is **physical partitioning** — one dataset per
visibility scope with rows duplicated across slices
(`docs/grimoire-kg-agenda.md:92-99`: "Facts visible to a *subset* of
players duplicate across those slices"). `#fut-fgac-subject-attribute`
records the general fine-grained-ACL idea as deliberately deferred.

## Decision record

Operator decisions, 2026-07-13 (brainstormed, committed):

1. **First-class catalog view object** — not a predicate on the type
   binding, not a pure ACL-side extension. A view is a virtual dataset in
   the catalog.
2. **Predicate + projection expressiveness** — view = base table +
   optional `RowFilter` predicate + optional column list. Not arbitrary
   SQL (no joins/aggregates) in v1.
3. **Write-through** — typed inserts/PATCH/delete through a view-bound
   type land in the base table, gated by the view predicate (a row you
   write must remain visible in your own view).
4. **Views are datasets** — same `(schema, name)` namespace as physical
   tables, own `DatasetRef`, grantable via the existing
   `PolicyTarget::Table`. No new ACL target kind.
5. **General platform capability** — no single driving consumer; success
   is the primitive existing end-to-end with tests. (Grimoire may adopt
   later; not gated on it.)
6. **Engine-side expansion; no view nesting in v1** (design calls,
   approved with the design).

## Context — what ships today (verified)

- **Binding**: `ingest/src/bind.rs:58-93` — dataset→type promotion
  validates the landed table's schema then `define_type`; the binding IS
  `ObjectType.table`, persisted in `ontology.object_type`
  (`postgres/migrations/0002_ontology.sql`, PK `name`, no uniqueness on
  the table columns → N types : 1 table is already legal).
- **ACL check is exact-match, deny-wins** (`acl.rs:403-409`); "P4 never
  resolves a `Type` to its backing `Table`" (`acl.rs:66-67`). Resolution
  fallbacks live in query-api: `DatasetVisibility`
  (`query-api/src/dataset_acl.rs:69-122`) allows a Table read on a Table
  allow **or** any backing type's allow, via a lazy N:1 `types_backed_by`
  reverse map.
- **Every read resolves type→table**: `resolve_governed`
  (`governed.rs:59-88`) → `compile_object_read` and friends use
  `g.otype.table` throughout `handler.rs`; the engine service is the sole
  serving path (`serving.rs`), resolving refs to Iceberg tables for
  DataFusion.
- **Row filters already fold into reads** (`governed.rs:103-122`
  `load_policy` → row_filters/denied/masked) **and writes**
  (`write_filter.rs:173-206` three-valued in-memory eval;
  `action.rs:169-173` coalesces multi-step actions by table equality).
- **`RowFilter` grammar** (`acl.rs:187-199`): `Compare/And/Or/Not` over
  literal `ScalarValue`s; for `Table` targets the `property` names a
  column directly — exactly the semantics a view predicate needs.
- **Reverse lookup hazard**: `identity_for_table`
  (`postgres/src/ontology.rs:681-684`) has no uniqueness guard when N
  types share a table — pre-existing; views must not widen it.

## Design

### 1. Catalog concern: the view object

New catalog surface, following the five-concern pattern (core trait +
memory fake + postgres adapter + testkit contract):

- `define_view(view: TableRef, base: TableRef, predicate:
  Option<RowFilter>, columns: Option<Vec<String>>)`. Validation at write:
  - `base` exists and is **physical** (no view-over-view in v1);
  - predicate properties and projection columns exist in the base schema
    (predicate `property` = base-table column, the existing
    `Table`-target `RowFilter` semantics);
  - `view` collides with no physical table and no other view — one
    namespace, one uniqueness rule.
- `resolve_dataset(ref) → Physical | View { base, predicate, columns }` —
  the single place consumers learn a dataset is virtual.
- `drop_view(ref)`; dropping a **base** table with dependent views is
  refused with the dependents listed (base-drop protection).
- `Catalog::schema(view_ref)` returns the base schema narrowed to the
  projection (no projection → full base schema).
- Storage: one migration, `catalog.dataset_view` (view schema/name PK,
  base schema/name, `predicate jsonb` — the existing `RowFilter` serde —
  `columns text[]` nullable). `.sqlx` refreshed via
  `tools/sqlx-prepare.sh`.

### 2. Serving: expansion in the engine

View expansion happens **engine-side**, at table-resolution time in the
serving layer — a view ref scans as
`SELECT <columns> FROM base WHERE <predicate>` (a DataFusion logical view
or rewritten provider). Rationale: every consumer gets correct expansion —
governed object reads, transform inputs over Flight, the deferred external
SQL wire — instead of each wire client folding it in. query-api stays a
zero-DataFusion wire client; ACL policies attached to the view target fold
**on top** in query-api exactly as today (view predicate AND policy row
filter; projection ∩ policy column deny/mask).

The engine scans the base under system authority — the caller needs only
the view (or view-bound type) grant, never the base grant.

### 3. Ontology & binding: nearly untouched

`ObjectType.table: TableRef` is unchanged and may now name a view ref.
`bind()` validates the type's properties against the view's *projected*
schema — same code path, since `Catalog::schema` already returns the
narrowed schema. `Ontology::resolve` still returns one ref.
Identity/merge resolution keys by the ref the type binds (the view ref);
the engine resolves through to the base for the physical scan. The
`identity_for_table` reverse lookup treats a view ref as just another
table key — no widening of the existing N:1 hazard.

### 4. ACL: no model change — that is the point

`PolicyTarget::Table(view_ref)` already works. Exact-match `check` gives
the decoupling for free: **a grant on the view is not a grant on the base
and vice versa**. `DatasetVisibility` / lineage naming treat views as
datasets with no new target kind (the Table∨backing-Type fallback applies
to view refs unchanged — a type bound to a view backs the view ref).
Fine-grained policies (row filter, column deny/mask) may attach to a view
target and compose with the view's own definition.

### 5. Write-through

Typed inserts, identity-targeted PATCH, and delete through a view-bound
type lower to the **base** table, gated by the view predicate:

- **Insert**: the row must satisfy the predicate (reuse the
  `write_filter.rs` three-valued eval; NULL/unknown ⇒ reject) and may only
  touch projected columns.
- **PATCH**: the target row is located within the view (base ∧ predicate ∧
  identity); the **post-image** must still satisfy the predicate — no
  writing a row out of your own view.
- **Delete**: identity match constrained by the predicate.
- Multi-step action deconfliction (`action.rs:169-173` table-equality
  coalescing) compares **resolved base** tables, so two views over one
  base correctly conflict.

### 6. Lineage and routes

- `define_view` emits a lineage edge base → view (a derivation node); the
  lineage closure and `/lineage` visibility treat the view as a dataset
  node under the existing readable predicate.
- `/datasets` routes list views as datasets with a `kind: view` marker;
  `get_dataset` returns the projected schema; `dataset_preview` applies
  predicate + projection (a view preview never shows rows outside the
  view).
- GC/compaction: views are metadata-only — no files, no snapshots; the
  only interaction is base-drop protection (§1).

## Non-regression

- Physical datasets, existing types, and all existing grants behave
  byte-identically — `resolve_dataset` returns `Physical` for everything
  that exists today.
- `PolicyTarget`, `Grant`, `Policy`, the ACL store schema, and
  `Acl::check` are unchanged — no ACL migration.
- `ObjectType` shape and the `define_type`/`bind` surfaces are unchanged.
- Stream/MV paths key physical tables and are untouched (subscribe-on-view
  deferred, below).

## Testing

- **Testkit contracts** for view CRUD + validation (collision, missing
  base, bad predicate column, view-over-view rejection, base-drop
  protection); memory + postgres both pass them.
- **Engine**: a scan through a view returns predicate/projection-narrowed
  rows; a scan of the base is unaffected.
- **query-api e2e** (reuse `//src/services/query-api:e2e-support` —
  `grant_read`, `subject_with_role`, seed helpers):
  - two types over two views on one base with disjoint grants — each role
    reads only its slice; neither can read the base or the sibling view;
    a base grant does not leak the views.
  - ACL policy on a view target composes (view predicate AND policy
    filter; mask applies within the projection).
  - write-through: insert satisfying the predicate lands in the base and
    reads back through the view; insert escaping the predicate → 403;
    PATCH moving a row out of the view → 403; delete constrained to the
    view.
  - `/datasets` lists the view (kind marker) under the caller's grants;
    preview is predicate/projection-narrowed; lineage shows base → view.
- Full `buck2 test //src/...` green; sqlx cache freshness via the
  existing `sqlx-cache-check`.

## Global constraints (loom-specific, carry into the plan)

- Strict clippy (pedantic + restriction); `#[expect(lint, reason)]`
  locally.
- `buck2 run //tools:prek -- run --all-files` before every commit.
- Tests are `rust_test`/`loom_fixture_test` targets only; fixture tests
  use `loom_fixture_test`.

## Out of scope (deferred — record as FUTURE items when this lands)

- **View-over-view nesting** — base must be physical in v1.
- **Stream/CDC subscribe on a view** — subscribe targets physical tables.
- **Arbitrary-SQL views** (joins/aggregates/renames) — read-only derived
  views are a different, larger primitive.
- **Per-subject dynamic filtering** — `#fut-fgac-subject-attribute` stays
  deferred; views cover *nameable static subsets* only. This spec relates
  to (does not close) that item.
- **Predicate-pushdown statistics** for view scans.

## Acceptance

1. One physical dataset can back N views, each independently grantable;
   a subject with a grant on one view reads exactly that slice — no row
   duplication, no base or sibling leakage.
2. A type binds to a view and the full governed surface works through it:
   object reads, link traversal, typed insert/PATCH/delete
   (predicate-gated), `/datasets`, lineage.
3. Everything that exists today is behavior-identical (full suite green).

## Interfaces (names the plan consumes)

- Consumes: `RowFilter` + `PolicyTarget` (`core/src/acl.rs:68-72,
  187-199`); `ObjectType`/`resolve` (`core/src/ontology.rs:39-54, 928`);
  `bind()` (`ingest/src/bind.rs:58-93`); `resolve_governed`/`load_policy`
  (`query-api/src/governed.rs:59-122`); `write_filter.rs` eval;
  `DatasetVisibility` (`query-api/src/dataset_acl.rs`); the engine
  serving resolution (`engine-serving/src/serving.rs`); the `/datasets`
  handlers (`query-api/src/http.rs`).
- Produces: the catalog view surface (`define_view` / `drop_view` /
  `resolve_dataset` / narrowed `schema`) across core/memory/postgres/
  testkit + migration; engine-side view expansion; view-aware write
  lowering; view-aware `/datasets` + lineage node; the e2e suite above.
