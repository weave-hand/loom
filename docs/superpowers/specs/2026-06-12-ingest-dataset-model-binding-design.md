# Design: dataset→model binding (Step 3, ingest — part 2b)

> **Status:** approved design (2026-06-12). Third sub-project of **Step 3 (ingest)**, the
> connective tissue between the landing materializer
> (`2026-06-11-ingest-materializer-primitive-design.md`, part 2a) and the governed read
> path (`2026-06-10-query-governed-object-read-slice-design.md`). It makes a landed
> dataset retrievable as a typed object through the query front door.

## Goal

Give loom the **validated promotion**: bind a landed DuckLake dataset (a real table the
materializer produced) to an **ontology type** the query FE can serve — checking the table's
**physical** schema actually satisfies the proposed type *before* persisting it. A bound type
is **guaranteed-serveable**: `read_object` can never fail on a missing or mistyped column.

This closes loom's two-layer model. The materializer lands abstract data with an inferred
schema (layer 1); the ontology is authoritative for typed retrieval (layer 2); **binding is
the gate between them** — the "this data *is* this model" check at promotion time, the mirror
of the materializer's ingest-time gate.

## Why this matters now

`Ontology::define_type` today **blindly upserts**: it records `(name, table_schema,
table_name)` + properties with **no** check that the table exists or that the properties
correspond to real columns. The read path (`read_object`) reads property *names* as columns
and runs SQL against the type's table, so a malformed type only fails at *query time* (the SQL
errors). Binding makes the model authoritative: a type is validated against the live physical
catalog (`Catalog::schema`) at bind time, so the governed read path is guaranteed to resolve.

## Architecture & placement

Two pieces, two homes:

- **`control-plane-core` gains loom's logical-type vocabulary** (`core/src/logical_type.rs`) —
  the base scalars, their DuckLake physical affinities, the semantic-alias table, and a pure
  `satisfies(logical_ty, physical_ty)` check. A genuine domain concept (loom's type system),
  pure logic, no I/O, reusable beyond binding.
- **`src/services/ingest` gains the `bind` orchestrator** (`bind.rs`) — holds `&dyn Catalog` +
  `&dyn Ontology`, runs the validation with core's vocabulary, and calls `define_type`. This
  is **ingest part 2b**: land (2a) → bind (2b) → queryable. Cross-concern orchestration in a
  service mirrors how `query-api`'s `read_object` composes ontology + acl + serving — no new
  `core` trait is needed.

### Flow

```
caller: ObjectType { name, properties:[{name, ty=LOGICAL, required}], table = landed TableRef }
   │
   ▼  bind(catalog, ontology, type_def)
  catalog.current_snapshot(table)         → TableNotFound if the dataset isn't live
  catalog.schema(table, snap.id)          → physical columns [{name, ty=DUCKLAKE, nullable}]
   │
   ▼  per property: column-present? + logical→physical satisfies? + required ⇒ non-nullable?
  collect ALL violations → DoesNotConform   (nothing persisted)
   │ else
   ▼
  ontology.define_type(type_def)          → the type is now serveable by read_object
```

## The logical-type vocabulary (`core/src/logical_type.rs`)

A small, opinionated, pure-logic module. Three parts.

**(a) Base scalars → DuckLake physical affinity** (exact-match, no implicit widening —
`Integer` is 32-bit, `Long` is 64-bit, deliberately distinct):

| `BaseType` | accepted DuckLake physical string(s) |
| --- | --- |
| `Integer` | `int32` |
| `Long` | `int64` |
| `Double` | `double` |
| `Boolean` | `boolean` |
| `String` | `varchar` |
| `Date` | `date` |
| `Timestamp` | `timestamp` |

These are loom's canonical DuckLake type strings (what the materializer's `duck_type` writes
and what DuckLake stores in `ducklake_column.column_type`). Per the DuckLake catalog-facts
gotcha, type strings carry casing quirks, so `satisfies` **normalizes** (trim + lowercase) both
sides before comparing. Timestamp-with-tz variants are deferred (this slice accepts bare
`timestamp`).

**(b) Semantic aliases → base** (small, opinionated, extensible — the Foundry-style semantic
layer):

| alias | base |
| --- | --- |
| `EmailAddress` | `String` |
| `Url` | `String` |
| `PhoneNumber` | `String` |

**(c) The pure API:**

```rust
pub enum BaseType { Integer, Long, Double, Boolean, String, Date, Timestamp }

impl BaseType {
    /// DuckLake physical type strings (lowercase) that satisfy this base type.
    pub fn physical_affinity(self) -> &'static [&'static str];
}

/// Resolve a logical type name (a base name or a known semantic alias,
/// case-insensitively) to its BaseType. `None` if loom doesn't know it.
pub fn resolve_logical(ty: &str) -> Option<BaseType>;

/// Does a DuckLake physical type string satisfy a logical type?
/// `Err(UnknownLogicalType)` if `logical_ty` is neither a base nor an alias.
pub fn satisfies(logical_ty: &str, physical_ty: &str) -> Result<bool, UnknownLogicalType>;

pub struct UnknownLogicalType(pub String);
```

So `satisfies("EmailAddress", "varchar")` → `Ok(true)`; `satisfies("Integer", "int64")` →
`Ok(false)` (32 vs 64-bit); `satisfies("Money", "double")` → `Err(UnknownLogicalType("Money"))`.
The vocabulary is **closed**: an unrecognized logical type is a binding error, never a silent
pass — that is what keeps the model authoritative.

### Out of scope (noted seam): JSON-out conformance

Binding never serializes; it validates logical↔**physical (DuckLake)**. How a value renders on
the wire is logical↔**JSON**, a different axis owned by the query/serving path (`read_object` →
`rows_to_json`). The base set was chosen to have obvious JSON renderings (Integer/Double →
number, Boolean → bool, String → string, Date/Timestamp → ISO-8601 string), so a **later
query-path typed-serialization slice** slots in cleanly. The one wrinkle to handle there:
`Long`/int64 exceeds JSON-number safe precision beyond 2^53, so a typed serializer will likely
render it as a JSON string. **This slice adds no wire encoding** to `core` or `bind`; the
vocabulary is merely the natural anchor for that future work.

## The `bind` operation (`src/services/ingest/src/bind.rs`)

```rust
pub async fn bind(
    catalog: &dyn Catalog,
    ontology: &dyn Ontology,
    type_def: ObjectType,            // name + properties (logical types) + target TableRef
) -> Result<(), BindError>;
```

**Validation** (collect ALL violations, like the materializer's gate):

1. `catalog.current_snapshot(&type_def.table)` → `BindError::TableNotFound` if not live.
2. `catalog.schema(&type_def.table, snap.id)` → the physical columns.
3. Per property, against its same-named physical column:
   - column absent → `MissingColumn`
   - `logical_type::satisfies(prop.ty, col.ty)`: `Err` → `UnknownLogicalType`; `Ok(false)` →
     `TypeMismatch { logical, physical }`; `Ok(true)` → ok
   - `prop.required && col.nullable` → `NullabilityViolation`
4. Any violations → `Err(BindError::DoesNotConform(Vec<BindViolation>))` — **nothing persisted**.
5. Else `ontology.define_type(type_def)` → serveable.

**Error type:**

```rust
pub enum BindError {
    TableNotFound(TableRef),
    DoesNotConform(Vec<BindViolation>),
    ControlPlane(#[from] control_plane_core::ControlPlaneError),
}
pub struct BindViolation { pub property: String, pub reason: BindViolationReason }
pub enum BindViolationReason {
    MissingColumn,
    UnknownLogicalType(String),
    TypeMismatch { logical: String, physical: String },
    NullabilityViolation,            // required property, nullable column
}
```

**Deliberate semantics & deferrals:**

- **Extra physical columns are fine.** A type is a *view* over the table; binding a narrower
  type (fewer properties than the table has columns) is allowed and common. Only declared
  properties are validated.
- **Validation is at bind time only.** The persisted type stores the `TableRef`, not a
  snapshot; the read path resolves at query-time's current snapshot. Incompatible later schema
  evolution is the deferred **ongoing-conformance / schema-evolution** concern.
- **TOCTOU:** a small window exists between validation and `define_type` where the table could
  change; acceptable this slice (schema evolution out of scope); noted.
- **No lineage-on-bind.** Recording "type X bound to dataset Y" would need a `Tx` ontology leg
  (does not exist); deferred.

## Testing

**(a) `core` vocabulary units** (`core/tests/logical_type.rs`, pure, fast): `resolve_logical`
for each base name (case-insensitive), each alias, and an unknown → `None`; `satisfies` with a
true case per base affinity, a false case (`Integer` vs `int64`), an alias case
(`EmailAddress`/`varchar`), an unknown → `Err(UnknownLogicalType)`, and physical-string
normalization (`"VARCHAR"` vs `"varchar"`).

**(b) `bind` validation** (`src/services/ingest/tests/bind.rs`, `loom_fixture_test(duckdb =
True)` against `PgFixture`): construct landed tables with precise physical schemas — the
materializer for realistic cases, `DuckLakeWriter` seed/`exec` to pin exact nullability/types
for rejection cases.
- **Accept:** a conforming `ObjectType` binds; afterward `get_type` returns it and `resolve`
  returns the table.
- **Reject — one test per violation + a multi-violation collection test:** `MissingColumn`;
  `TypeMismatch` (`Integer` property over an `int64` column); `UnknownLogicalType` (property
  `ty = "Money"`); `NullabilityViolation` (`required` property over a nullable column);
  `TableNotFound`.
- **Nothing persisted on rejection:** after a failed bind, `get_type` returns `NotFound`.

**(c) Connective-tissue e2e** (lives in **`query-api`**'s test suite — it has the governed-read
harness: `PgFixture` + `EmbeddedDuckDb` serving + ACL + `read_object` — with a **test-only dep
on `ingest`**): `materialize` a dataset → `bind` a type → grant a Read ACL → `read_object`
returns the rows. The executable proof that landed-then-bound data is queryable through the
governed front door. `query-api → ingest` is test-only and acyclic in production deps.

**Adapter handle:** `bind` takes `&dyn Catalog` + `&dyn Ontology`; the plan confirms the
postgres adapter exposes both on one fixture handle (it co-locates all concerns).

## Verification

- `buck2 test //src/...` green; the bind accept/reject matrix passes against the real catalog;
  the materialize→bind→read_object e2e returns rows.
- `tools/clippy-all.sh` clean; `buck2 run //tools:prek -- run --all-files` green.

## Scope / non-goals

- **In:** the `core` logical-type vocabulary (base scalars, affinities, semantic aliases,
  `resolve_logical`/`satisfies`); the `bind` orchestrator (validate-then-`define_type`) with
  collected `BindViolation`s; the accept/reject test matrix; the materialize→bind→read e2e.
- **Out (later slices):** auto-deriving a draft type from a dataset; **JSON-out / typed wire
  serialization** (query path); lineage-on-bind; ongoing-conformance / schema-evolution
  re-validation; links between types; multi-table types; richer semantic vocabulary.

## Open risks

- **DuckLake type-string drift.** The affinity map compares against DuckLake's canonical type
  strings; casing/aliasing quirks are mitigated by normalization, but a DuckDB version bump
  could introduce a new spelling. Mitigated by the bind tests running against the real pinned
  catalog and the materializer writing canonical strings.
- **Bind-time-only validation.** A type valid at bind time can be invalidated by later schema
  evolution; out of scope here but flagged so the ongoing-conformance slice is not a surprise.
- **Vocabulary completeness.** The closed base + alias set is deliberately small; a property
  using an unlisted semantic type errors. This is the authoritative stance, but the alias table
  will need extension as real ontologies grow — an additive change.
