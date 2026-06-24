# Define-time ontology validation — implementation plan

> **For agentic workers:** implement task-by-task under TDD (`rust_test`
> integration targets only — no inline `#[cfg(test)]`). Steps use checkbox
> (`- [ ]`) syntax for tracking.
> **Spec:** `docs/superpowers/specs/2026-06-23-define-time-ontology-validation-design.md`
> **Item:** `road-define-time-ontology-validation`.

**Goal:** Reject — at authoring time, with a collect-all-violations error naming
each bad reference — a `define_type` whose derived properties reference a missing
link / target / agg-column or an inapplicable aggregation, and a `define_link`
whose backing columns do not exist in the endpoint/join tables. Validation reads
the catalog through the existing `bind` seam (`&dyn Catalog` + `&dyn Ontology`);
the `Ontology` trait stays decoupled from the catalog. No behavior change for
ontologies that are already defined and valid.

**Tech stack:** Rust (edition 2024), `control_plane_core` traits + logical-type
vocabulary, the `control_plane_memory` fakes for in-memory unit tests, the
`control_plane_postgres` hermetic fixture for postgres parity, buck2 +
`loom_fixture_test` / `rust_test`.

## Spec deviation (MUST follow the code, not the prose)

The spec's `bind_link` prose says: "`from_column` exists on the from-type's
table, `to_column` on the to-type's table, and `from_key`/`to_key` exist on the
join table." **This is backwards.** The authoritative `LinkBacking::JoinTable`
join condition (`core/src/ontology.rs:60-68`) and the read-time SQL
(`query-api/src/sql.rs:734-742`) are:

```
from_table.from_key = join_table.from_column
AND join_table.to_column = to_table.to_key
```

Corroborated by every real test fixture (e.g. `graph_path_e2e.rs:184-189`:
Person.id=`from_key`, membership.person_id=`from_column`, membership.team_id=
`to_column`, Team.id=`to_key`). The **correct** `JoinTable` column→table mapping
this plan validates is therefore:

| Field         | Table it must exist in |
|---------------|------------------------|
| `from_key`    | the **from-type's** table |
| `from_column` | the **join** table |
| `to_column`   | the **join** table |
| `to_key`      | the **to-type's** table |

Validating against the wrong table would reject valid links and accept invalid
ones, so we implement the table above and note the spec-prose error in the PR.

## File structure

| File | Responsibility | Action |
|------|----------------|--------|
| `src/services/ingest/src/bind.rs` | New `BindViolationReason` variants; derived-property validation in `bind`; sibling `bind_link`; small private agg/type-class helpers | Modify |
| `src/services/ingest/src/lib.rs` | Export `bind_link` from the crate | Modify |
| `src/services/ingest/tests/bind_validation.rs` | In-memory unit tests (memory `Catalog`+`Ontology`) for derived validation + `bind_link` | Create |
| `src/services/ingest/tests/bind.rs` | Add postgres-parity cases (one derived, one `bind_link`) to the existing DuckLake fixture suite | Modify |
| `src/services/ingest/BUCK` | New `bind-validation` `rust_test` target (deps `:ingest`, `//src/control-plane/{core,memory}`, `//third-party:tokio`) | Modify |

No new SQL — the validators use only existing `Catalog::current_snapshot` /
`Catalog::schema` and `Ontology::links` / `resolve` reads, so **no `.sqlx` cache
change** is expected (confirm with a clean `git diff` if any query macro is
touched — none should be).

## Global constraints

- **Tests are `rust_test` integration targets only.** No inline `#[test]` in
  `src/**.rs` (the `no-inline-tests` prek hook fails the build). The new
  in-memory suite is a pure-logic `rust_test` (RE-eligible, like `materialize`);
  the postgres-parity additions go in the existing `loom_fixture_test` `bind.rs`.
- **Collect-all violations** (mirror `bind`): a `define_type`/`define_link` is
  rejected with *every* violation found, never one-at-a-time. Persist nothing on
  rejection; persist via `define_type`/`define_link` only when clean.
- **`Ontology` trait unchanged** — no catalog handle threaded into the trait; the
  coupling stays in the `bind` module seam (Decision of the spec).
- **Behaviour-preserving for valid ontologies** — the read-time omission/400
  paths in `query-api/src/handler.rs` are untouched.
- **Run the FULL suite** `buck2 test //src/...` (redirect to a file + grep, never
  pipe `buck2 test` through `tail`/`head`). Build with `BUILDBUDDY_API_KEY` set
  (RE on); the fixture tests route their command local via `loom_fixture_test`.
- No `Cargo.toml`/dependency changes → no reindeer run, no lockfile drift.

---

## Task 1: New violation variants + agg/type-class helpers

**File:** `src/services/ingest/src/bind.rs` (modify)

Extend `BindViolationReason` (currently `bind.rs:25-33`) with four variants
(all `Debug, Clone, PartialEq, Eq` — String fields satisfy `Eq`):

```rust
/// A derived property names a link not defined (outbound) on this type.
UnknownDerivedLink(String),
/// A derived aggregation's column is absent from the link target's table.
MissingAggColumn,
/// An aggregation is not applicable to its column's logical type
/// (Sum/Avg need numeric; Min/Max need an ordered type).
BadAggType { agg: String, column: String },
/// A derived property's declared result type is unknown or inconsistent with
/// the aggregation's result category.
BadDerivedResultType { declared: String, expected: String },
```

Add private helpers in the module:

```rust
/// The target-type column an aggregation reads (None for Count).
fn agg_column(agg: &Aggregation) -> Option<&str> { /* Count->None; Sum/Avg/Min/Max(c)->Some(c) */ }
/// A human label for an aggregation, for violation messages.
fn agg_label(agg: &Aggregation) -> &'static str { /* "Count"/"Sum"/... */ }
/// Sum/Avg apply to numeric base types.
fn is_numeric(b: BaseType) -> bool { matches!(b, BaseType::Integer | BaseType::Long | BaseType::Double) }
/// Min/Max apply to any totally-ordered base type (everything except Boolean).
fn is_ordered(b: BaseType) -> bool { !matches!(b, BaseType::Boolean) }
```

**Interfaces produced:** the new variants + helpers, consumed by Task 2.

- [ ] Add the four variants and four helpers. Imports needed in `bind.rs`:
  add `Aggregation, BaseType, DerivedPropertyDef, LinkBacking, LinkDef, PageReq,
  TableSchema, TypeName, resolve_logical` to the `control_plane_core` use.

## Task 2: Derived-property validation in `bind`

**File:** `src/services/ingest/src/bind.rs` (modify)

After the existing reserved-name loops and **before** the
`if !violations.is_empty()` return (`bind.rs:128`), add a derived-property pass.
Fetch the type's outbound links once (only when `derived` is non-empty); a type
not yet defined has no links (`NotFound` → empty), so every derived link is
unknown — this encodes the spec's authoring order (define types → links → bind
with derived):

```rust
if !type_def.derived.is_empty() {
    let links = match ontology.links(&type_def.name, PageReq::unbounded()).await {
        Ok(p) => p.items,
        Err(ControlPlaneError::NotFound(_)) => Vec::new(),
        Err(e) => return Err(BindError::ControlPlane(e)),
    };
    for d in &type_def.derived {
        validate_derived(catalog, ontology, &links, d, &mut violations).await?;
    }
}
```

**When are `links` empty?** Only when the from-type was never `define_type`'d —
then `Ontology::links` returns `NotFound`, which we map to an empty slice so every
derived link reports `UnknownDerivedLink`. In the normal authoring order (types →
links → bind-with-derived) the from-type *is* defined, so `links` returns the
real outbound list and each derived link is checked against it.

`validate_derived` (private async helper, returns `Result<(), BindError>`; pushes
to `violations`):

1. **Link exists.** `links.iter().find(|l| l.name == d.link)`. Absent ⇒ push
   `UnknownDerivedLink(d.link)`, return `Ok(())`.
2. **Agg column (column-bearing aggs only).** If `agg_column(&d.agg)` is `Some(c)`:
   - `ontology.resolve(&link.to)` → target `TableRef`; `NotFound` ⇒ push
     `MissingAggColumn` and return (target gone ⇒ column can't exist).
   - `catalog.current_snapshot(&target_table)`; `NotFound` ⇒ push
     `MissingAggColumn`, return. Other err ⇒ `Err(ControlPlane)`.
   - `catalog.schema(&target_table, snap.id)`; find column `c`. Absent ⇒ push
     `MissingAggColumn`, return.
   - `col_base = resolve_logical(&col.ty)`. Applicability: Sum/Avg require
     `is_numeric`, Min/Max require `is_ordered`. Inapplicable (or `col_base` is
     `None`) ⇒ push `BadAggType { agg: agg_label, column: c }` (continue to the
     result-type check — collect-all).
3. **Result type consistency.** `declared = resolve_logical(&d.ty)`; ok when:
   - `Count` → `declared ∈ {Integer, Long}`;
   - `Sum`/`Avg` → `declared` is numeric;
   - `Min`/`Max` → `declared.is_some() && declared == col_base`.
   Not ok ⇒ push `BadDerivedResultType { declared: d.ty, expected }` where
   `expected` is `"integer or long"` (Count), `"numeric"` (Sum/Avg), or the
   column's `canonical_name()` (Min/Max; fall back to `"the target column's type"`
   if `col_base` is `None`). The violation's `property` is `d.name` throughout.
   **Spec note:** the spec prose says "Count → integer"; we accept `Integer` *or*
   `Long` because a count is naturally an int64 — the valid test declares Count →
   `Long`. This intentional prose relaxation is documented in the PR (alongside the
   JoinTable deviation); the `expected` label `"integer or long"` keeps the error
   message consistent with what is actually accepted.
   **Note:** `agg_label` returns `&'static str` but the `BadAggType.agg` /
   `BadDerivedResultType` fields are `String` — `.to_string()` / `.into()` at the
   push sites.

- [ ] **Test first** (`tests/bind_validation.rs`, memory fakes): write the failing
  cases below, then implement until green.
  - valid derived (`Count` link → Long; `Sum(amount:double)` → Double) round-trips
    and persists;
  - `UnknownDerivedLink` (derived names an undefined link);
  - `MissingAggColumn` (`Sum` over a column absent from the target table);
  - `BadAggType` (`Sum` over a `string` column / `Avg` over `boolean`);
  - `BadDerivedResultType` (`Count` declared `String`; `Min(date)` declared `Long`);
  - collect-all: one `define_type` yielding ≥2 derived violations.
  Each in-memory test: `MemoryControlPlane::new(..)`, `seed_catalog` the
  from/target tables, `define_type` the base from-type (WITHOUT the derived
  property) + target type, `define_link` the link, then `bind` the from-type with
  the derived property added.
- [ ] Implement `validate_derived`; assert **nothing persisted on rejection**.
  **Oracle care:** because the from-type was `define_type`'d first (so
  `define_link` could attach the link), `get_type(from)` *succeeds* after a
  rejected re-`bind` — so do NOT assert `NotFound`. Instead assert the rejected
  *derived property was not stored*: `get_type(from).await?.derived` is still empty
  (unchanged from the pre-bind shape). Reserve the `get_type → NotFound` oracle for
  a brand-new type that fails its *first* bind (no link/derived involved).

## Task 3: `bind_link` sibling validator

**File:** `src/services/ingest/src/bind.rs` (modify) + `src/lib.rs` (export)

```rust
pub async fn bind_link(
    catalog: &dyn Catalog,
    ontology: &dyn Ontology,
    link: LinkDef,
) -> Result<(), BindError>;
```

Two private helpers:

```rust
async fn schema_of_table(catalog: &dyn Catalog, table: &TableRef) -> Result<TableSchema, BindError>;
// current_snapshot (NotFound -> BindError::TableNotFound(table)) then schema(table, snap.id).
async fn schema_of_type(catalog, ontology, ty: &TypeName) -> Result<TableSchema, BindError>;
// ontology.resolve(ty)? (missing endpoint type -> ControlPlane(NotFound)); then schema_of_table.
```

**Deliberate error asymmetry (add a code comment):** a missing *endpoint type*
surfaces as `ControlPlane(NotFound)` (endpoint types are `define_link`'s
precondition, already validated there), whereas a missing *join table* surfaces as
`BindError::TableNotFound` (a user-facing backing reference, matching `bind`'s
own table-not-found). Document this so a later reader doesn't "fix" it to one form.

Body — collect-all `MissingColumn` violations (reuse the existing variant; the
violation's `property` names the offending column):

- **`ForeignKey { from_column, to_column }`** — `from_column` in
  `schema_of_type(from)`, `to_column` in `schema_of_type(to)`.
- **`JoinTable { table, from_key, from_column, to_column, to_key }`** — resolve
  all three schemas first (`schema_of_type(from)`, `schema_of_type(to)`,
  `schema_of_table(table)`; a missing join `table` ⇒ `BindError::TableNotFound`,
  matching `bind`). Then per the **corrected** mapping above: `from_key` in
  from-schema, `from_column`+`to_column` in join-schema, `to_key` in to-schema.

Persist via `ontology.define_link(link)` only when `violations` is empty.

- [ ] Export: `pub use bind::{BindError, BindViolation, BindViolationReason, bind, bind_link};`
  in `src/services/ingest/src/lib.rs`.
- [ ] **Test first** (`tests/bind_validation.rs`): FK good (both columns exist) +
  bad-from-column + bad-to-column; JoinTable good (correct mapping) + one bad
  column per position (`from_key`, `from_column`, `to_column`, `to_key`) +
  join-table-not-in-catalog (`TableNotFound`); assert nothing persisted on
  rejection (`ontology.links` does not contain the link). Implement until green.

## Task 4: Postgres parity + BUCK wiring

**Files:** `tests/bind.rs` (modify), `BUCK` (modify)

- [ ] **BUCK:** add a `rust_test` named `bind-validation` (NOT `loom_fixture_test`
  — pure in-memory), `crate = "bind_validation"`, `crate_root =
  "tests/bind_validation.rs"`, `edition = "2024"`, deps `[":ingest",
  "//src/control-plane/core:core", "//src/control-plane/memory:memory",
  "//third-party:tokio"]`. Mirror the existing `materialize` target's shape.
- [ ] **Postgres parity** in the existing `bind.rs` `loom_fixture_test`: add
  (a) one derived-property case that *passes* validation against the real
  DuckLake catalog (seed a target table + `define_link`, bind a from-type with a
  valid `Count`/`Sum` derived), proving the adapter's logical-type mapping feeds
  the validator; (b) one `bind_link` FK case (good + a bad column) over the real
  catalog. Keep these minimal — the in-memory suite carries the exhaustive matrix.

## Verification

- [ ] `buck2 build //src/services/ingest/...` clean (incl. `[clippy.txt]` empty).
- [ ] `buck2 test //src/...` fully green (redirect to file + grep
  `Tests finished|FAIL`). The new `bind-validation` runs on RE; `bind` +
  other fixtures route local.
- [ ] `git diff --stat` shows no `.sqlx` change; no `Cargo.toml`/`Cargo.lock`
  drift.
- [ ] Acceptance: a bad derived reference and a bad `define_link` backing column
  are rejected at authoring time with a collect-all error; valid ontologies are
  unaffected; the read-time handler is unchanged.

## Out of scope (unchanged from spec)

Stored/authored chain construct (folded/dropped); the full coercion taxonomy
(`fut-coercion-taxonomy` — existence + agg-result category only); a networked
`define_link` endpoint (this provides the validator it will call);
`BindViolation`/conformance `Violation` consolidation; update/delete & custom
actions.
