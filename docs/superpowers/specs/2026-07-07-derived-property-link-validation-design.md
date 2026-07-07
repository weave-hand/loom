# Derived-property link & column validation — design

**Item:** `#iss-delete-link-derived-dangle`

## Problem

A derived property (aggregate-over-link, `DerivedPropertyDef` in
`src/control-plane/core/src/ontology.rs:263`) names a link by string
(`DerivedPropertyDef.link`) and stores it FK-less: `ontology.derived_property`
has `link_name text not null` with **no** foreign key to `ontology.link`
(`src/control-plane/postgres/migrations/0010_derived_property.sql:6`). Two
surfaces silently corrupt reads as a result.

**Surface 1 — delete strands the derived column.** The governed read
resolves each derived property's link among the owning type's outbound links
and, when the link is missing, *omits* the column with no error
(`src/services/query-api/src/handler.rs:317-319`). So `Ontology::delete_link`
(`core/src/ontology.rs:782`; postgres `postgres/src/ontology.rs:150-160`;
memory `memory/src/ontology.rs:57-63`) and its route
`DELETE /admin/links/{from}/{name}` (`src/services/runtime/src/admin.rs:806-821`)
can make a bind-validated derived column vanish from every reader's result with
no warning to the admin.

**Surface 2 — define never checks the agg column.** `define_type` writes the
derived rows verbatim (postgres `postgres/src/ontology.rs:86-103`; memory
`memory/src/ontology.rs:22-40`; the admin `DefineModelReq` even notes the gap,
`admin.rs:513-514`). The read path guards a *missing* link, a gone target type,
and a *denied* target column (`handler.rs:317-336`) but never a resolving link
whose `agg` names a **nonexistent** target column: that compiles `SUM(sub."nope")`
(quoted, injection-safe) and fails in DataFusion at query time — an opaque 500
for every reader of the type.

## Scope

Two additions: (1) a 409 referrer guard on `delete_link` that hard-blocks the
delete while any derived property names the link; (2) define-time validation of
a derived property's `agg` column existence and numeric-ness (for `Sum`/`Avg`)
against the link's resolved target type. Both adapters (memory + postgres) plus
the testkit ontology contract, and the two admin routes.

**Non-goals (explicit):**
- **No FK schema migration.** `ontology.derived_property.link_name` stays
  FK-less; a referrer *read* plus define-time validation covers the defect
  without a DB constraint. An FK would break `delete_link`'s
  "definition-only / idempotent / physical untouched" contract and force a
  cross-type define ordering the ontology deliberately does not impose.
- **No retroactive validation** of already-stored bad derived properties beyond
  the existing read-path omit/guard behavior — validation applies to new
  `define_type` writes only.
- **No read-path change.** The omit-on-missing-link and denied-column guards in
  `handler.rs:317-336` stay exactly as-is; this spec closes the write side.
- Memory/postgres adapter parity is **required** — the testkit contract pins
  both to identical behavior.

## Design

### 1 · Referrer guard (Surface 1)

**New Ontology read.** Add to the trait
(`core/src/ontology.rs`, near `delete_link:782`):

```
/// Names of derived properties on `from` whose link is `name`. Empty if none.
async fn derived_properties_referencing(&self, from: &TypeName, name: &str)
    -> Result<Vec<String>>;
```

A derived property resolves its link among **its own** type's outbound links
(`handler.rs:306` calls `ontology.links(&type_name)`), and links are keyed
`(name, from)` — so only derived properties **on `from`** can strand when link
`(from, name)` is deleted. The read is therefore scoped to `type_name = from`:
- **postgres:** `select name from ontology.derived_property where type_name = $1
  and link_name = $2 order by ordinal` — a new committed `.sqlx` entry
  (regenerate via `tools/sqlx-prepare.sh`).
- **memory:** filter `types.get(from).derived` by `d.link == name`.

**Guard in `delete_link` (both adapters).** Before deleting, call the read; if
non-empty, return `ControlPlaneError::Conflict(msg)` where `msg` lists the
referring derived-property names — *no delete happens*. This runs independent of
link existence (the read hits `derived_property`, not `link`), so a
still-referenced link that is already physically gone still 409s, forcing
cleanup; idempotency is preserved once no derived property names it.

**409 response shape.** `status_for` already maps `Conflict → 409`
(`src/services/runtime/src/auth.rs:45`), but it renders a bare status. Give
`delete_link_route` (`admin.rs:806-821`) a `Conflict` branch that returns 409
with a JSON body naming the blockers, e.g.
`{"error":"link `customer` is referenced by derived properties","derived":["lifetimeSpend"]}`;
all other errors keep the `status_for(&e)` idiom. Document the 409 in the
route's `#[utoipa::path]` responses.

### 2 · Define-time agg validation (Surface 2)

The typing machinery already exists in core and is unit-tested but unwired:
`Aggregation::column()` (`ontology.rs:315`), `Aggregation::column_applicable()`
(`ontology.rs:341`, `Sum`/`Avg` ⇒ numeric, `Min`/`Max` ⇒ ordered), and
`resolve_logical(ty) -> Option<BaseType>` (`logical_type.rs:144`).

**New core validator.** A free function
`validate_derived_columns(derived, resolve_target: impl Fn(&str) -> Option<&ObjectType>)`
that, for each derived property carrying a column (`agg.column().is_some()`):
- resolves the link's target type via the caller-supplied closure. If the link
  is **not resolvable** (link undefined, or target type not yet defined at
  define time) ⇒ **skip** — best-effort, so a type-with-derived can still be
  defined before its link/target exist (the read path keeps guarding the
  missing link). This is a deliberate choice, not an ordering constraint.
- if resolvable: require the `agg` column to exist among the target type's
  `properties`; absent ⇒ `Err(Validation("derived `X`: link `L` target `T` has
  no column `C`"))`. For `Sum`/`Avg`, additionally require
  `column_applicable(resolve_logical(col.ty))` (numeric) ⇒ else
  `Err(Validation(... not numeric ...))`.

**Both adapters call it inside `define_type`**, beside the existing
`validate_constraints` gate (postgres `ontology.rs:17`; memory `ontology.rs:25`):
the adapter supplies the resolver by looking up each link among the type's own
outbound links (self-links resolve against `ty` itself; others against stored
types/links in the same tx / lock). The error surfaces as
`ControlPlaneError::Validation` ⇒ **400** via `status_for`
(`auth.rs:46`), matching `define_model`'s documented 400 (`admin.rs:525-526`)
and `POST /admin/models` — no route change beyond the doc note.

## Testing

**Testkit ontology contract** (`src/control-plane/testkit/src/lib.rs`, extending
the derived-property block near `:1256` and the `delete_link` block near
`:1430`), so memory and postgres are pinned identically:
- `delete_link` on a link named by a derived property returns `Conflict`; the
  derived property is unchanged; after redefining the owning type *without* that
  derived property, `delete_link` succeeds — then re-delete is `Ok` (idempotent
  again).
- `derived_properties_referencing` returns the referring name(s), and `[]` for
  an unreferenced link.
- `define_type` with a `Sum`/`Avg`/`Count`+column derived property naming a
  **nonexistent** target column (link + target resolvable) ⇒ `Validation`.
- `define_type` with a `Sum` over a **non-numeric** target column ⇒ `Validation`.
- `define_type` whose derived link/target is **not yet defined** ⇒ `Ok`
  (deferred validation), and the read path still omits it.

**Admin HTTP tests** (`src/services/runtime`):
- `DELETE /admin/links/{from}/{name}` for a referenced link ⇒ **409** with a
  body listing the derived properties; an unreferenced link ⇒ 200 as today.
- `POST /admin/models` with a derived property whose agg column is missing /
  non-numeric (against a defined link+target) ⇒ **400**.
