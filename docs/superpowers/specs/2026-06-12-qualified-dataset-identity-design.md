# Design: qualified dataset identity (control-plane coupling)

> **Status:** approved design (2026-06-12). Addresses the lone unresolved **high-severity**
> finding in `2026-06-06-control-plane-critical-review.md` (Coupling [H] #1 / five-things #4):
> cross-concern dataset identity is convention-coupled strings — coupled in reality, uncoupled
> in the compiler. This slice makes the catalog↔lineage join **typed and centralized**;
> referential validation stays deferred, exactly as the review allows.

## Goal

Introduce `DatasetId` — loom's canonical, deployment-independent identity for a dataset it
governs (a physical DuckLake table) — and centralize the one conversion that today is
hand-built in every service: `TableRef` ↔ the lineage `DatasetRef` for loom's own namespace.
After this, renaming a table changes one typed value, and the string that joins a lineage edge
to a catalog table is constructed in exactly one place, under test — instead of `format!`'d with
a hardcoded namespace at every call site.

## Why this matters now

`core` has three identity types that *should* agree but don't, in the compiler's eyes:

- `TypeName(String)` (ontology) and `TableRef { schema, name }` (catalog) — already reused by
  ACL's `PolicyTarget::{Type, Table}`, so **ACL identity is already typed**.
- `DatasetRef { namespace, name }` (lineage) — a free OpenLineage string pair with **no typed
  link** to `TableRef`/`TypeName`. This is the gap.

A lineage edge naming `main.customer` and an `ObjectType` bound to `TableRef { schema: "main",
name: "customer" }` are joined *only* by a string that ingest call sites build by hand
(`namespace: "loom-ingest"`, `name: format!("{schema}.{name}")`). Rename the table and every
lineage edge silently dangles, with zero compiler help — and the namespace + name format are
copy-pasted across three call sites today (`ingest/tests/{ducklake_interop,materialize}.rs`,
`query-api/tests/bind_read_e2e.rs`).

The review explicitly scoped the fix: *"Even without full validation, a shared newtype for
qualified dataset identity + a documented namespacing convention would make the coupling
visible."* That is exactly this slice.

## Reconciling the deliberate decoupling

`core`'s `DatasetRef` doc comment makes a deliberate argument *against* centralizing this: an
OpenLineage `namespace` is datasource-derived (`s3://bucket`, `postgres://host:port`), so the
`TableRef → DatasetRef` mapping "depends on deployment context… belongs to the consuming
services, not to `core`."

The reconciliation: that argument holds for **external** datasets (real sources feeding ingest),
but **not** for datasets *loom itself governs*. A loom table's *logical* identity does not change
with which Postgres host backs the catalog. So loom declares **one canonical logical namespace
for its own datasets** — a `core` constant — and centralizes only that half of the mapping. The
deployment-dependent, external half stays free-form `DatasetRef`s owned by services, untouched.
This reverses only the part of the decoupling that was never actually deployment-dependent.

## The type (`core/src/identity.rs`)

```rust
/// loom's canonical logical namespace for datasets it governs. Deployment-independent:
/// a loom table's logical identity is stable regardless of which Postgres host backs the
/// catalog. External datasets (s3://bucket, postgres://host) keep their own datasource-
/// derived namespaces and are NOT loom-namespaced.
pub const LOOM_DATASET_NAMESPACE: &str = "loom";

/// loom's canonical identity for a dataset it governs — a physical DuckLake table.
/// The deployment-independent logical identity that bridges catalog `TableRef` and lineage
/// `DatasetRef`, so the two stop being joined by hand-built strings. An ontology type reaches
/// its dataset through `ObjectType.table -> DatasetId`, so no separate `Type` variant is
/// needed; a type-level variant is an additive change if type-level lineage ever lands.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct DatasetId(TableRef);

impl DatasetId {
    /// The physical table this dataset identity refers to.
    pub fn table(&self) -> &TableRef;

    /// The OpenLineage identity for this loom dataset: the loom namespace plus a
    /// dot-qualified `schema.name`.
    pub fn dataset_ref(&self) -> DatasetRef;

    /// Parse a `DatasetRef` back into a loom `DatasetId`. `None` when the ref is not
    /// loom-namespaced (i.e. it names an external dataset) or its name is not a
    /// well-formed `schema.table`.
    pub fn from_dataset_ref(dr: &DatasetRef) -> Option<DatasetId>;
}

impl From<&TableRef> for DatasetId { /* wrap */ }

/// Convenience for call sites that just want the lineage ref for a loom table.
impl From<&TableRef> for DatasetRef { /* via DatasetId::dataset_ref */ }
```

### Name convention

The `DatasetRef.name` for a loom table is `"{schema}.{name}"` (e.g. `"main.customer"`), parsed
back with `split_once('.')`. **Documented constraint:** loom schema/table identifiers contain no
`.` (they're simple SQL identifiers), so the round-trip is unambiguous. `from_dataset_ref`
returns `None` rather than panicking on a name with no `.` or an empty side.

### Round-trip contract

- `DatasetId::from(&table).dataset_ref()` →
  `DatasetRef { namespace: "loom", name: "<schema>.<name>" }`.
- `DatasetId::from_dataset_ref(&dr)` is the inverse for loom-namespaced refs:
  `from_dataset_ref(&from(&table).into()) == Some(DatasetId::from(&table))`.
- A `DatasetRef` whose namespace is anything else (`"s3://bucket"`, `"postgres://h"`,
  `"loom-ingest"`-from-the-old-world) → `None`.

## Adoption (this slice)

`materialize` takes a pre-built `LineageEvent` from its caller (the function does *not* construct
the output `DatasetRef` itself — the caller, which knows the datasource, supplies the event). So
adoption is at the **call sites that build the output `DatasetRef`**:

- The three hand-built sites (`ingest/tests/ducklake_interop.rs:47`, `ingest/tests/materialize.rs:27`,
  `query-api/tests/bind_read_e2e.rs:56`) switch from
  `DatasetRef { namespace: "loom-ingest".into(), name: "main.<t>".into() }` to
  `DatasetRef::from(&table)` — identical name, namespace canonicalized to `"loom"`.

This is the honest adoption surface today (the eventual networked ingest shell will build its
output dataset ref the same way). `materialize`'s signature and lineage's trait / `DatasetRef`
storage are **unchanged** — `DatasetId` is a construction/bridge helper, so the slice is fully
additive and non-breaking.

## Testing

**(a) `core` round-trip matrix** (`core/tests/identity.rs`, pure, fast):
- `DatasetId::from(&TableRef{"main","customer"}).dataset_ref()` ==
  `DatasetRef { namespace: "loom", name: "main.customer" }`; `LOOM_DATASET_NAMESPACE == "loom"`.
- Inverse: `from_dataset_ref` of that ref == `Some(DatasetId::from(&table))`; `.table()` returns
  the original `TableRef`.
- External namespace (`"s3://bucket"`, `"postgres://h"`) → `None`.
- Malformed loom-namespaced names (`"nodot"`, `".x"`, `"x."`, `""`) → `None` (no panic).
- A non-default schema (`TableRef{"analytics","orders"}`) round-trips.

**(b) ingest adoption** (the updated tests): the existing `materialize` / interop / bind-read e2e
tests pass unchanged in behavior with the conversion in place; add an assertion in the
materializer test that the emitted output `DatasetRef` equals `DatasetRef::from(&table)` (locks
the call site to the canonical conversion).

## Verification

- `buck2 test //src/...` green; the identity round-trip matrix passes; the three adopted tests
  pass with the canonical `"loom"` namespace.
- `tools/clippy-all.sh` clean; `buck2 run //tools:prek -- run --all-files` green.

## Scope / non-goals

- **In:** the `DatasetId` newtype + `LOOM_DATASET_NAMESPACE` + the centralized `TableRef ↔
  DatasetRef` conversions and parse-back; the round-trip test matrix; adopting it at the three
  hand-built call sites.
- **Out (later slices):**
  - **Referential validation** — rejecting a lineage edge / policy that names a table or type
    that doesn't exist. The review explicitly defers this; `DatasetId` is the seam it will hang on.
  - **Property/column identity** — `RowFilter::Compare.property` ↔ `PropertyDef.name` ↔
    `ColumnDef.name` are still bare strings. A weaker coupling (a filter naturally names a
    property by string); a natural follow-up, not this slice.
  - **Type-level dataset identity** — an `enum DatasetId { Table(TableRef), Type(TypeName) }` for
    the type-level lineage `ARCHITECTURE.md` gestures at; additive when/if that lands.
  - **Migrating the `DatasetRef` storage shape** — lineage keeps persisting `DatasetRef`; this
    slice only centralizes its construction.

## Open risks

- **Namespace change `"loom-ingest"` → `"loom"`.** Only three test call sites use the old string
  and there is no persisted production lineage, so this is a safe canonicalization. Flagged so
  the rename is a conscious choice, not a silent drift; `from_dataset_ref` deliberately returns
  `None` for the old `"loom-ingest"` namespace (it is not the canonical loom namespace).
- **Dotted identifiers.** The `schema.name` round-trip assumes no `.` in loom identifiers. True
  for DuckLake schema/table names today; documented as a constraint, and `from_dataset_ref`
  degrades to `None` rather than mis-parsing.
