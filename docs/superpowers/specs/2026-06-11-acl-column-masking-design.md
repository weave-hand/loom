# Design: column masking (redaction marker)

> **Status:** approved design (2026-06-11). Second slice of **Step 3, full ACL
> semantics** (after explicit-deny / deny-override, `2026-06-10-acl-deny-override-design.md`).
> Adds a softer column control alongside `deny_columns`.

## Goal

Add `mask_columns` to ACL policies: a **softer** control than `deny_columns`. A
masked column stays in the result but every value is replaced with a fixed
redaction marker, and the column is unfilterable. This is distinct from
`deny_columns`, which removes the column entirely (absent + unfilterable). Masking
deliberately reveals that a value exists while hiding it; deny hides its existence.

## Scope: one slice of full ACL semantics

Takes ONLY the redaction-marker column mask. Explicitly out of scope (later slices
or separate features):

- **NULL or hash mask strategies** — only the redaction marker this slice.
- **Per-column or configurable markers** — one global constant.
- **Masking ontology links / foreign keys** — links are not traversable in the read
  path yet (the deferred "rich ontology" query work); column masking here is
  scalar-only.
- **Deny on fine policies, role hierarchy** — separate slices.

## Semantics

- A masked column is emitted as the constant marker `'***'`: `compile_select` emits
  `'***' AS "col"` instead of `"col"`. Because the marker is a constant, **no value
  is ever read** from the masked column (leak-proof by construction), and the result
  column is **text** regardless of the source type (the accepted retype).
- **Unfilterable:** a request `eq_filter` targeting a masked column is rejected with
  `QueryError::BadFilter`, exactly as for a denied column — a caller cannot probe the
  real value through a filter. (Join/link leakage is moot: link traversal does not
  exist in the read path yet.)
- **Deny wins over mask:** a column that is both denied and masked is dropped. Deny
  removes it from `allowed`; mask only applies to columns still in `allowed`.
- **Union across policies:** `mask_columns` are unioned across the subject's matching
  policies, the same way `deny_columns` are.
- A fully-masked type still returns rows (all markers) — NOT `Forbidden`. `Forbidden`
  remains reserved for "no visible columns at all" (everything denied) and the
  deny-by-default access gate.

## Core model (`src/control-plane/core/src/acl.rs`)

`Policy` gains a field:
```rust
pub struct Policy {
    pub target: PolicyTarget,
    pub row_filter: Option<RowFilter>,
    pub deny_columns: Vec<String>,
    /// Columns shown but value-masked (redacted to a marker). Distinct from
    /// `deny_columns`, which removes the column. Order unspecified.
    pub mask_columns: Vec<String>,
}
```
`set_policy` stores it and `policies_for` returns it. The rest of the fine-policy
layer (row_filter, deny_columns) is unchanged.

## Adapters (both, behind the shared contract)

- **postgres** (`src/control-plane/postgres`): migration `0006_acl_policy_mask.sql`
  adds `mask_columns text[] not null default '{}'` to `acl.policy`. `set_policy`'s
  upsert inserts/updates `mask_columns`; `policies_for`'s select returns it. Via
  compile-time `query!`; **regenerate the committed `.sqlx` cache**
  (`tools/sqlx-prepare.sh`), keep `sqlx-cache-check` green.
- **memory** (`src/control-plane/memory`): already stores the whole `Policy` struct,
  so it carries `mask_columns` automatically once the field exists.

## Enforcement (query-api)

- **`read_object`** (`src/services/query-api/src/handler.rs`): in the policy-gather
  loop, also union each policy's `mask_columns` into a `masked` set. After computing
  `allowed = properties − denied`, intersect: `masked = masked ∩ allowed` (deny wins).
  Reject any `eq_filter` whose column is in `masked` (in addition to the existing
  not-in-`allowed` rejection) → `BadFilter`. Pass `allowed` and `masked` to
  `compile_select`.
- **`compile_select`** (`src/services/query-api/src/sql.rs`): add a `mask_cols:
  &[String]` parameter (positioned right after `allowed_cols`). When building the
  projection, for each column in `allowed_cols`: emit `'<marker>' AS "col"` if the
  column is in `mask_cols`, else the bare `quote_ident(col)`. The marker is a single
  `const MASK_MARKER: &str = "***";` inlined as a SQL string literal — it is a
  compile-time constant, never caller data, so inlining it is not an injection vector
  (all *caller* values remain bound `?` params as today). An empty `mask_cols`
  produces byte-identical SQL to the current implementation.

## Testing

- **testkit acl contract** (`src/control-plane/testkit/src/lib.rs`): `set_policy`
  with a non-empty `mask_columns` round-trips through `policies_for` on both the
  in-memory fake and real Postgres (extend the existing policy round-trip
  assertions; existing assertions add `mask_columns: vec![]` where they construct a
  `Policy`).
- **sql.rs unit tests** (`src/services/query-api/tests/sql_compile.rs`): a masked
  column emits `'***' AS "col"` in its projection position; a mix of plain + masked
  columns preserves order (e.g. `SELECT "id", '***' AS "secret" FROM ...`); existing
  tests gain the empty `&[]` mask arg and keep their exact expected SQL.
- **query-api `governed_read`** (`src/services/query-api/tests/governed_read.rs`):
  a policy that masks `secret` (instead of denying it) → the result includes a
  `secret` column whose every value is the marker `***` (not the real `s1`/`s3`),
  while `id`/`status` are real; and a request `eq_filter` on `secret` →
  `QueryError::BadFilter`.

## Migration / call-site impact

`Policy` gains a field, so every `Policy { ... }` literal must add `mask_columns`
(the testkit contract's policy constructions, the query-api `governed_read` oracle's
`set_policy` call, and any acl adapter tests). postgres migration `0006` is the next
free number (existing: 0001–0005); `acl.policy` rows default `mask_columns` to `'{}'`
(backward-compatible, no backfill). `compile_select`'s new `mask_cols` parameter
updates its existing unit-test call sites (add `&[]`).

## Non-goals (restated)

NULL/hash mask strategies; configurable/per-column markers; link/FK masking; deny on
fine policies; role hierarchy; Type↔Table target resolution; tenancy.
