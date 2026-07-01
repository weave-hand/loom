# Filter error contract — richer `BadFilterValue` body + 422 for action params

- **Date:** 2026-07-01
- **Area:** query
- **Register items:** promotes [[fut-richer-filter-error]] + [[fut-422-body-endpoints]] → mints [[road-filter-error-contract]]
- **Status:** spec (ready for a work agent to plan + build)

## North star

Two error-contract improvements on the query/action write surface: an **uncoercible filter
value** reports the *expected type* and the *offending value* (not just a column name / opaque
parse string), and **`POST /actions`** returns **422** for a well-formed body that fails
semantic parameter validation (reserving 400 for malformed/undecodable requests), aligning
with the `road-model-constraints` 422 already used on the action write path.

## Current state

**Filter value errors.** An uncoercible filter value is rejected in `handler.rs:274` when
`coerce_predicate` returns a `FilterError`, surfaced as `QueryError::BadFilterValue`
(`handler.rs:82`, `#[from] FilterError`) and mapped to **400** at `http.rs:177` with the body =
the error's `Display` string (e.g. `"filter amount: expected a number: invalid float
literal"`). A separate `BadFilter(String)` (`handler.rs:76`, visibility denial) is also 400
with body = column name. The value error already *names* the column and a parse detail, but the
contract is an opaque string, not a structured `{column, expected, value}`.

**Action param errors.** `POST /actions` maps `ActionError::BadParams(ParamError)`
(`action.rs:34`) to **400** at `http.rs:773`, and the OpenAPI doc (`http.rs:709`) documents
"Bad params" as 400 — even though the body is well-formed JSON that failed *semantic*
validation (wrong type, missing required param), which is the textbook 422 case. The action
write path already returns **422** for `road-model-constraints` violations, so the two
semantic-validation failures on the same endpoint currently disagree (400 vs 422).

## Design

### Richer `BadFilterValue` body

`FilterError` (the coercion error in `filter.rs`) is enriched to carry the structured triple
it already half-has: **`column`**, **`expected`** (the property's logical type name — the same
vocabulary `coerce_predicate` coerces against), and **`value`** (the offending raw operand
string, echoed back so the caller sees what was rejected). The `http.rs:177` mapping stays
**400** (an uncoercible URI filter is a malformed request) but renders a structured body:

```
400  { "error": "bad_filter_value",
       "column": "amount", "expected": "double", "value": "abc" }
```

The `Display` string is kept for logs; the HTTP body becomes the structured form. `BadFilter`
(visibility) is unchanged (column name only — never leaks a value the caller can't see; it is a
permission signal, not a coercion signal).

### 422 for action param validation

`POST /actions` `BadParams` moves from 400 → **422**:

- **Malformed / undecodable** request (not valid JSON, wrong shape at the envelope level) stays
  **400** — the request itself is bad.
- A **well-formed** body whose params fail *semantic* validation (missing required param, type
  mismatch, uncoercible value) returns **422 Unprocessable Entity** — the request was
  understood but the parameters can't be processed.

Update the status mapping (`http.rs:773`) and the OpenAPI response doc (`http.rs:709`) to
document 422 (semantic) + 400 (malformed). This aligns `BadParams` with the
`road-model-constraints` 422 on the same write path, so both semantic-validation failures speak
one status.

### Decided (not open)

- **GET typed-filter stays 400** — a body-less GET with an uncoercible URI param is a malformed
  request; only the *body* is enriched, the status is right.
- **`POST /actions` semantic failures → 422**, malformed JSON → 400.
- **`BadFilter` (visibility) is untouched** — no value echo (avoids leaking a rejected value's
  shape on a permission denial); only `BadFilterValue` (coercion) gets the value.
- **Structured JSON error body** for `BadFilterValue`; the `Display` string remains for logs.

## Scope

In scope: enrich `FilterError` to `{column, expected, value}` + render a structured 400 body
at the `BadFilterValue` mapping; flip `POST /actions` `BadParams` to 422 for semantic failures
(400 for malformed) + update the OpenAPI response doc + the status mapping; e2e assertions on
both bodies/statuses.

Out of scope: reworking the whole error envelope/format across the API (a broader effort);
changing `BadFilter` visibility semantics; 422 on any other endpoint; i18n of error messages;
structured bodies for errors other than `BadFilterValue` (kept minimal to the two items).

## Testing

`typed_filter_e2e.rs` + `action_e2e.rs` (`loom_fixture_test`):

1. **Richer filter body:** a `?amount=gt:abc` returns **400** with body `{column:"amount",
   expected:"double", value:"abc"}`; a valid filter is unaffected.
2. **Visibility unchanged:** a denied-column filter still returns 400 with column name only, no
   value echo.
3. **422 semantic action:** `POST /actions` with a well-formed body missing a required param /
   wrong-typed param returns **422** (was 400); the body names the offending param.
4. **400 malformed action:** a non-JSON / envelope-malformed action body still returns **400**.
5. **Alignment:** a `road-model-constraints` violation (422) and a `BadParams` semantic failure
   (422) on the same endpoint now share a status class; a client can treat 422 as "fix your
   inputs".
6. **OpenAPI:** the generated doc for `POST /actions` documents 422 + 400.

## Risk

- **Small and localized** — two error mappings in `http.rs` + a struct enrichment in
  `filter.rs`. No SQL, no governance, no write-path logic changes.
- **The 422 flip is a client-visible status change** — mitigated by scoping it to *semantic*
  failures (malformed stays 400) and aligning with the existing 422 on the same path; documented
  in OpenAPI (6). Any client treating 4xx generically is unaffected; one asserting exactly 400
  on bad params must update — noted as the one back-comat consideration.
- **The value echo in `BadFilterValue`** is safe because it echoes the caller's **own** input,
  and only on a *coercion* error (never on a visibility denial), so no unseen data leaks — test
  (2) pins that boundary.
