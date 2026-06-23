# Define-time ontology validation — design

_2026-06-23. Work item: [[road-define-time-ontology-validation]] (promoted from
[[fut-define-link-validation]] + [[fut-define-time-derived-validation]]; folds
[[fut-define-time-chain-validation]])._

## Problem

The ontology write path stores references it never checks against the physical
catalog, so a bad reference surfaces late — at read/traversal time — instead of
at authoring time:

1. **`define_link` physical columns.** `define_link` validates only that both
   endpoint *types* exist (`memory/src/ontology.rs:30–41`,
   `postgres/src/ontology.rs:78–120`); it stores the `LinkBacking` column names
   (`ontology.rs:51–99`: FK `from_column`/`to_column`, or JoinTable
   `from_key`/`from_column`/`to_column`/`to_key`) **without checking those columns
   exist**. A bad column surfaces at traversal time. This was deferred deliberately
   (Decision 1 of `2026-06-14-query-governed-link-traversal-design.md`) to keep the
   ontology write path decoupled from a catalog read.

2. **`define_type` derived properties.** A `DerivedPropertyDef { name, ty, link,
   agg }` (`ontology.rs:126–132`) names a link to traverse and an aggregation over
   the target. `define_type` validates none of it. At read time the query handler
   **silently omits** the derived property if the link is missing
   (`services/query-api/src/handler.rs:263–264`, `// missing link -> omit (no
   define-time validation in part-1)`), if the target type is missing
   (`:271–274`), or if the agg column is ACL-denied (`:279–282`).

3. **Multi-hop chains.** A caller-supplied traversal chain resolves at read time;
   a broken chain surfaces as a 400 (`QueryError::BadChain`/`UnknownLink`,
   `handler.rs:44–81`), not at authoring.

`bind` (`services/ingest/src/bind.rs:37`) is **already** the define-time
conformance gate for `define_type`: it takes `&dyn Catalog, &dyn Ontology`,
resolves `current_snapshot(table)` → `catalog.schema(table, snap.id)`
(`catalog.rs:74`, returns `TableSchema { columns: Vec<ColumnDef> }`), validates
every declared *property* against its same-named physical column (collecting all
`BindViolation`s, `bind.rs:62–90`), and persists via `define_type` **only when
clean** (`bind.rs:128–133`). Derived properties slip through it today — only their
reserved-`_` name is checked (`bind.rs:119–126`). This slice extends that existing
seam rather than introducing a new coupling.

## Goal

Reject — at authoring time, with a collect-all-violations error naming each bad
reference — a `define_type` whose derived properties reference a missing
link/target/column or an inapplicable aggregation, and a `define_link` whose
backing columns do not exist in the endpoint tables. Validation reads the catalog
through the existing `bind` seam; the `Ontology` trait stays decoupled from the
catalog (preserving Decision 1's boundary). No behavior change for ontologies that
are already defined and valid.

## Decision — validate at the seam, not in the trait

The chosen coupling is a **validation seam** taking `&dyn Ontology` + `&dyn
Catalog`, invoked before `define_*` — not a catalog handle threaded into the
`Ontology` trait. `bind` already *is* this seam, so:

- **Derived-property validation extends `bind`** (it already holds catalog +
  ontology + the type).
- **Link validation is a sibling `bind_link`** in the same module, mirroring
  `bind`'s shape, since `define_link` has no live caller yet (it is test-only
  today). `bind_link` becomes the canonical authoring path the eventual networked
  define_link endpoint will call, exactly as the ingest path calls `bind`.

The memory `Ontology`/`Catalog` fakes stay as they are (the memory fake already
implements both concerns, `memory/src/catalog.rs:44`), so the seam is testable
fully in-memory.

## Derived-property validation (extend `bind`)

Add a pass over `type_def.derived` alongside the existing property loop. For each
`DerivedPropertyDef { name, ty, link, agg }`:

1. **Link exists on this type.** Look up `link` among `ontology.links(type_def.name)`.
   Absent ⇒ violation. (This makes authoring order explicit — see below.)
2. **Target resolves.** Resolve the link's `to` type → its backing `TableRef`
   (via `ontology.get_type`/`resolve`).
3. **Agg column exists & is applicable.** `Aggregation` (`ontology.rs:112–124`):
   `Count` takes no column; `Sum`/`Avg`/`Min`/`Max(col)` require `col` to exist in
   the target table's schema (`catalog.schema(target_table, target_snap.id)`) and
   the agg to be type-applicable — numeric for `Sum`/`Avg`, ordered for
   `Min`/`Max`.
4. **Result type is consistent.** Declared `ty` must be a known logical type and
   consistent with the agg's result category: `Count` → integer, `Min`/`Max` →
   the column's type, `Sum`/`Avg` → numeric. Existence + category only; the full
   coercion lattice defers to [[fut-coercion-taxonomy]].

New `BindViolationReason` variants (extending `bind.rs:25–33`), reported with the
existing collect-all behavior so an author fixes everything in one pass:

- `UnknownDerivedLink(String)` — the named link is not defined on this type;
- `MissingAggColumn` — the agg column is absent from the target table;
- `BadAggType { agg: String, column: String }` — the agg is not applicable to the
  column's type;
- `BadDerivedResultType { declared: String, expected: String }` — declared `ty`
  is unknown or inconsistent with the agg result.

The `property` field of each `BindViolation` carries the derived property's `name`.

## Link validation (`bind_link`)

A sibling free function, same module and shape as `bind`:

```rust
pub async fn bind_link(
    catalog: &dyn Catalog,
    ontology: &dyn Ontology,
    link: LinkDef,
) -> Result<(), BindError>;
```

Endpoint *types* are already checked by `define_link`; `bind_link` adds the
physical-column gate, then persists via `define_link` only when clean:

- **`LinkBacking::ForeignKey { from_column, to_column }`** — resolve `link.from` →
  its table, check `from_column` exists in that schema; resolve `link.to` → its
  table, check `to_column` exists.
- **`LinkBacking::JoinTable { table, from_key, from_column, to_column, to_key }`** —
  the join `table` must be live in the catalog; `from_column` exists on the
  from-type's table, `to_column` on the to-type's table, and `from_key`/`to_key`
  exist on the join table.

Reuses `BindViolation`/`BindError::DoesNotConform`; a missing backing column is a
`MissingColumn` violation whose `property` names the offending column. A join
table absent from the catalog is `BindError::TableNotFound`, matching `bind`.

## Authoring order (decision)

Derived-property validation checks the link against **current ontology state**: a
link that is not yet defined is a violation, not a silent pass. The explicit
authoring order is therefore **define types → define links → bind type with
derived properties**. This is acceptable discipline (it mirrors the read-time
requirement that the link must exist for the derived property to resolve) and
avoids a chicken-and-egg between a type's derived property and a link whose
`from`/`to` reference that type.

## Folding the chain item

[[fut-define-time-chain-validation]] is **subsumed** and marked `status:dropped`:

- Once every link is column-validated and endpoint-typed at define time, any chain
  composed of *defined* links is structurally sound by construction.
- Ad-hoc multi-hop chains are caller-supplied **query paths** with no stored
  artifact to validate at authoring time. Their read-time `BadChain`/`UnknownLink`
  400 (`handler.rs`) is the correct surface and remains.

So there is no separate define-time chain artifact to validate; the per-link gate
plus the existing read-time check fully account for the item.

## Testing

- **`bind` unit tests** (ingest; memory `Catalog` + memory `Ontology`): a valid
  derived property round-trips; one case per new violation
  (`UnknownDerivedLink`, `MissingAggColumn`, `BadAggType`, `BadDerivedResultType`),
  plus collect-all (a single `define_type` yielding multiple derived violations).
  Seed the target table schema in the memory catalog and the link in the memory
  ontology.
- **`bind_link` unit tests**: FK and JoinTable backings, good case + a bad column
  per position, and a join-table-not-in-catalog case.
- **Postgres parity**: exercise both validators through the existing hermetic-PG
  fixture targets (`postgres/BUCK` ontology/bind suites). The validators use only
  existing `Catalog::schema` / `Ontology` reads — **no new SQL**, so no `.sqlx`
  cache change is expected (confirm with `tools/sqlx-prepare.sh` and a clean
  `git diff` if any query macro is touched).
- **No regression**: the read-time omission/400 paths in `handler.rs` are
  unchanged for already-defined, valid ontologies; an existing e2e is sufficient
  to confirm valid derived properties still serve.

## Out of scope

- A stored/authored **chain** construct (no such artifact exists; folded above).
- The full **coercion taxonomy** ([[fut-coercion-taxonomy]]) — this validates
  existence + agg-result category, not the complete logical/physical coercion
  lattice.
- A **networked `define_link` endpoint** — this slice provides the `bind_link`
  validator that endpoint will call; the endpoint itself is separate.
- Consolidating `BindViolation` with the conformance `Violation` enum
  ([[fut-conformance-enum-consolidation]]).
- Update/delete and custom-logic actions ([[fut-update-delete-actions]],
  [[fut-custom-logic-actions]]).
