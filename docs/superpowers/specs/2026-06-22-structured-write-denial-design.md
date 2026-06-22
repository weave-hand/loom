# Structured action write-denial reason

_Design spec. 2026-06-22._

## Context

When a governed action's fine-grained Write policy denies an insert, the
`run_action` handler already computes a precise reason but throws it away: it
collapses the rich `WriteVerdict` to a generic `ActionError::Forbidden` and logs
the reason server-side only, returning a **bodyless 403**
(`src/services/query-api/src/action.rs:177-190`,
`src/services/query-api/src/http.rs:607`). The comment at `action.rs:160` records
this was deliberate ("the body stays a generic 403; the reason is logged only").
A caller therefore cannot tell *why* their write was denied —
`[[fut-structured-write-denial]]`.

This slice surfaces a **caller-scoped** structured reason in the 403 body:
enough for a legitimate caller to self-correct, without disclosing the policy's
row-filter expression. It is a small, self-contained query-api change — no
control-plane, ontology, or ACL-model changes.

## Current state

- **`WriteVerdict`** (`src/services/query-api/src/write_filter.rs:153-161`)
  already carries the reason:
  ```rust
  pub enum WriteVerdict { Allow, DenyColumn(String), DenyRow }
  ```
  `check_write_policy` returns `DenyColumn(col)` (an inserted column is in some
  policy's `deny_columns`) or `DenyRow` (the row fails some policy's
  `row_filter`).
- **`run_action`** (`action.rs:177-190`) matches the verdict, logs, and returns
  `ActionError::Forbidden` for both — discarding `col` and the column/row
  distinction.
- **`ActionError`** (`action.rs:23-40`): `Forbidden` is a unit variant.
- **HTTP mapping** (`http.rs:607`):
  `ActionError::Forbidden => StatusCode::FORBIDDEN.into_response()` — empty body.
  (Sibling errors like `Misconfigured` already return a `403`/`500` *with* a
  detail body, so a bodied 403 is an established shape here.)
- **Coarse Write gate**: before the fine-grained check, the subject must clear
  `acl.check(subject, Action::Write, target)` (a deny-by-default coarse gate).
  That denial is a separate `Forbidden` return; see scope note below.

## Decision — caller-scoped disclosure

The 403 body distinguishes the two denial kinds and names the offending
**column** (which the caller already supplied in their own request, so naming it
discloses nothing the caller doesn't know). It does **not** reveal the
`row_filter` predicate that failed — that expression stays server-side, exactly
the confidentiality the current logs-only stance protects.

Response body (JSON), `403 Forbidden`:

```json
// column denial
{ "error": "write_denied", "reason": "column", "column": "<name>" }
// row-filter denial
{ "error": "write_denied", "reason": "row_filter" }
```

- `reason` is `"column"` or `"row_filter"`.
- `column` is present only for `"column"` denials.
- The row-filter predicate, the policy id, and which role's policy denied are
  **never** in the body (still logged server-side via the existing
  `tracing::info!` lines, which are retained).

This delivers the FUTURE item's intent (column-level reason + the
column-vs-row-filter distinction) while keeping policy structure confidential.

## Implementation

1. **A structured error variant.** Add to `ActionError`:
   ```rust
   #[error("write denied")]
   WriteDenied(WriteDenialReason),
   ```
   where `WriteDenialReason` is a small query-api enum mapping the verdict:
   `Column(String)` and `RowFilter`. (Keep the existing unit `Forbidden` for the
   coarse-gate and other denials — see scope.)
2. **`run_action`** (`action.rs:177-190`): keep the `tracing::info!` lines; return
   `ActionError::WriteDenied(WriteDenialReason::Column(col))` /
   `WriteDenied(WriteDenialReason::RowFilter)` instead of `Forbidden`.
3. **HTTP mapping** (`http.rs`): add an arm
   `ActionError::WriteDenied(reason) => (StatusCode::FORBIDDEN, Json(body(reason))).into_response()`
   serializing the body above. The existing `Forbidden => 403` arm stays for the
   coarse gate.
4. No change to `check_write_policy`, the ACL model, or the control plane — the
   verdict already exists; this only stops discarding it.

## Error handling / semantics

- **Fail-closed unchanged.** The *decision* to deny is untouched (UNKNOWN still
  denies, deny-column checked before row-filter); only the *rendering* of an
  already-made denial changes.
- **Precedence.** `check_write_policy` returns the first column denial before any
  row-filter check, so a column denial is reported in preference to a row-filter
  denial — the body reflects the verdict the gate actually produced (no new
  precedence logic).
- **Stable contract.** `error: "write_denied"` is a stable machine-readable tag; a
  caller can branch on `reason` without parsing prose.

## Testing

`rust_test` integration targets, extending the existing action/write-filter tests
(reuse `//src/services/query-api:e2e-support`).

- **Column-denial body**: an action setting a `deny_columns` column returns `403`
  with `{ "error": "write_denied", "reason": "column", "column": "<name>" }`.
- **Row-filter-denial body**: an action whose row violates a Write `row_filter`
  returns `403` with `{ "error": "write_denied", "reason": "row_filter" }` and
  **no** `column` field and **no** predicate text (assert the predicate string
  does not appear in the body — the confidentiality guarantee).
- **Allow unchanged**: a permitted action still returns `201` with the row.
- **Coarse-gate denial unchanged**: a subject without the coarse Write grant still
  gets the existing `403` (bodyless or its current shape) — this slice does not
  change that path (scope note).
- **Reason mapping unit test**: `WriteVerdict` → `WriteDenialReason` → body JSON
  for both kinds, a pure shaping test.

## Scope boundary

- **In:** the structured `WriteDenied` variant + body for the **fine-grained**
  Write-policy denial (column / row-filter), HTTP mapping, tests. Caller-scoped
  disclosure (column name only).
- **Out (deferred, tracked):** disclosing the row-filter predicate / policy id
  (deliberate non-goal — confidentiality; could be a future operator-gated
  verbose mode); restructuring the **coarse** Write-gate `403` (and the read-side
  `Forbidden`s in `handler.rs`) into structured bodies — a broader error-envelope
  pass left to a follow-up so this slice stays minimal; a uniform JSON error
  envelope across all query-api endpoints (`[[fut-richer-filter-error]]`,
  `[[fut-422-body-endpoints]]`).

## Acceptance criteria

1. A fine-grained Write-policy denial returns `403` with a JSON body carrying
   `error: "write_denied"` and `reason: "column"|"row_filter"`, plus `column` for
   column denials.
2. The body never contains the row-filter predicate, policy id, or role — only
   the caller-scoped reason; the predicate remains in the server log only.
3. Allow, coarse-gate denial, and all other action outcomes are unchanged.
4. `buck2 test //src/...` is green.
