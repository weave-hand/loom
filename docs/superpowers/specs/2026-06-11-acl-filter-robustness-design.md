# Design: ACL filter robustness (fallible compile + write-time validation)

> **Status:** approved design (2026-06-11). Fifth slice of **Step 3, full ACL
> semantics** (after deny-override, column masking, role hierarchy). Hardens the
> RowFilter path: a malformed filter can never panic a read, and `set_policy` rejects
> malformed/invalid filters at write time.

## Goal

Close the two robustness gaps in the ACL row-filter path:
1. **Read backstop:** `compile_select` currently `panic!`/`unreachable!`s on a
   `CompareOp`↔`ScalarValue` mismatch (e.g. `In` with a non-list value). Since a
   `RowFilter` is deserialized from `jsonb` policy data, a malformed-but-type-valid
   filter could panic inside a read request. Make `compile_select` fallible — return a
   typed error, surfaced as an HTTP 500, never a panic.
2. **Write validation:** `set_policy` stores any filter without checking it. Validate
   it at write time — structurally always, and (for a Type target) against the ontology
   type's properties — rejecting bad filters with a typed `Validation` error.

A single shared validator in `control-plane-core` is the one definition of filter
well-formedness, used by both paths.

## Scope

- One source-of-truth validator `validate_row_filter` in core.
- Fallible `compile_select` (query-api read path).
- `set_policy` write-time validation on both adapters (structural + Type-target
  property existence), strict on the type existing.
- New `ControlPlaneError::Validation(String)` variant.

Out of scope (later / separate):
- **Value-type matching** — checking a leaf's `ScalarValue` against the physical column
  type. (Needs the catalog schema; deferred.)
- **`grant` target validation** — grants have no filter; coarse target-existence is a
  separate deferred concern.
- **Table-target column existence** — Table targets reference columns directly with no
  registry; structural validation only.
- **Validating filters written outside `set_policy`** — the read backstop covers those.

## The shared validator (`src/control-plane/core/src/acl.rs`)

```rust
use std::collections::HashSet;

/// Validate a RowFilter's well-formedness. Always checks structure; when
/// `properties` is `Some`, also requires every `Compare` leaf's `property` to be a
/// member. Returns a human-readable reason on the first failure.
///
/// Structural rules (the CompareOp <-> ScalarValue invariant):
/// - `In` / `NotIn`            => value MUST be `ScalarValue::List`
/// - `Eq/Ne/Lt/Le/Gt/Ge`       => value must NOT be a `ScalarValue::List`
/// - `IsNull` / `IsNotNull`    => value is ignored
/// Recurses through `And` / `Or` / `Not`.
pub fn validate_row_filter(
    f: &RowFilter,
    properties: Option<&HashSet<String>>,
) -> Result<(), String>;
```

Lives in `core` next to `RowFilter` so both the adapters (`set_policy`) and the
query-api (`compile_select`) depend on one definition. Unit-tested in core.

## Read backstop — fallible `compile_select` (`src/services/query-api/src/sql.rs`)

- A small error type:
  ```rust
  #[derive(Debug, thiserror::Error)]
  pub enum CompileError {
      #[error("malformed row filter: {0}")]
      MalformedFilter(String),
  }
  ```
- `compile_select(...) -> Result<(String, Vec<SqlValue>), CompileError>`. Up front, for
  each ACL row filter it calls `validate_row_filter(f, None)` (structural) and maps
  `Err(reason)` → `CompileError::MalformedFilter(reason)`. After validation the
  SQL-building match arms that previously `panic!`/`unreachable!`d are genuinely
  unreachable — keep them as `unreachable!("validated above")` with a comment, or
  build directly knowing the shape is valid.
- `read_object` (`handler.rs`): `let (sql, params) = compile_select(...)?;` with a new
  `QueryError` variant:
  ```rust
      #[error(transparent)]
      Malformed(#[from] crate::sql::CompileError),
  ```
  mapped in `http.rs` to **`StatusCode::INTERNAL_SERVER_ERROR` with an opaque body**
  (a malformed stored policy is a server-side data-integrity fault, not a client
  error — and the existing opaque-500 arm already avoids leaking internals).

## Write validation — `set_policy`

New core error variant (additive on the `#[non_exhaustive]` `ControlPlaneError`,
reserved for exactly this in Step 2a #4):
```rust
    /// A request or stored value failed validation (e.g. a malformed/invalid RowFilter).
    #[error("validation error: {0}")]
    Validation(String),
```

`set_policy(role, policy)` on both adapters, after the existing role-exists `NotFound`
check, when `policy.row_filter` is `Some(f)`:
- **`PolicyTarget::Table(_)`:** `validate_row_filter(f, None)` (structural only) → on
  `Err(reason)` return `ControlPlaneError::Validation(reason)`.
- **`PolicyTarget::Type(name)`:** look up the ontology type on the same adapter.
  - type **not defined** → `ControlPlaneError::Validation("policy references unknown type {name}")` (strict).
  - type defined → build the `HashSet<String>` of its property names; `validate_row_filter(f, Some(&props))` → `Err(reason)` → `Validation`.

Then proceed with the existing upsert. (A `None` row_filter skips validation.)

### Adapters
- **memory** (`src/control-plane/memory/src/acl.rs`): `set_policy` reads the ontology
  state for the type's properties via a private helper
  `type_properties(&self, name: &TypeName) -> Option<Vec<String>>` (`None` = undefined
  type). Then validates.
- **postgres** (`src/control-plane/postgres/src/acl.rs`): `set_policy` runs one query
  against the `ontology` schema to fetch the target type's property names (empty/no rows
  ⇒ treat as undefined → `Validation`); then validates. Compile-time `query!`;
  **regenerate the committed `.sqlx` cache** (`tools/sqlx-prepare.sh`); keep
  `sqlx-cache-check` green. (The existing `get_type`/ontology queries show the schema;
  fetch property names for the `(type)` target.)

## Testing

- **core unit tests** (`validate_row_filter`): structural Ok/Err for each op × value
  shape (`In` + list Ok; `In` + scalar Err; `Eq` + list Err; `Eq` + scalar Ok;
  `IsNull` + any Ok); property-set Ok/Err (`Some` with a known vs unknown leaf
  property); nested `And`/`Or`/`Not` recursion (a deep malformed leaf is caught).
- **testkit acl contract** (both adapters): `set_policy` →
  - malformed filter (`In` + scalar value) on a defined Type → `Validation`;
  - unknown leaf `property` on a defined Type → `Validation`;
  - any filter on an **undefined** Type → `Validation`;
  - well-formed filter with known properties on a **defined** Type → `Ok` (define the
    type first);
  - well-formed filter on a `Table` target → `Ok` (no property check).
- **query-api** unit (`sql_compile.rs`): a malformed filter → `Err(MalformedFilter)`
  (no panic); valid filters → unchanged SQL (existing assertions keep their exact
  strings; calls now `.unwrap()` / `?` the `Result`).

## Migration / call-site impact

- **Strict-validation fallout (important):** existing `set_policy` call sites that use a
  **Type target with a `row_filter`** will now fail `Validation` unless the type is
  defined first. Audit and fix:
  - the **testkit acl contract** policy round-trips (currently set policies on
    `ttype("Customer")`/`ttype("Invoice")` with row filters and `deny`/`mask` columns —
    define those types in the contract before `set_policy`, OR switch those that only
    exercise columns to `Table` targets);
  - the **query-api `governed_read` oracle** already defines the `Order` type before
    `set_policy`, so its row-filter policy is fine — confirm.
- `compile_select` becoming `Result` updates its `sql_compile.rs` test call sites
  (`.unwrap()`) and the single `handler.rs` caller (`?`).
- No new migration (the `ontology` schema already holds types + properties).
- `ControlPlaneError::Validation` is additive (the enum is `#[non_exhaustive]`).

## Non-goals (restated)

Value-type matching; grant target validation; Table-target column existence; validating
non-`set_policy`-written filters (read backstop covers them); Type↔Table resolution.
