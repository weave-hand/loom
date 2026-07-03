# Classify `/search` engine-derived coercion faults as internal, not caller 400

- **Date:** 2026-07-03
- **Area:** query
- **Register item:** [[iss-qa-search-badfiltervalue-classification]]
- **Status:** approved

## Problem

`vector_search` (`src/services/query-api/src/handler.rs:567`) post-filters the
engine's kNN hits through row policy: it stringifies the ENGINE's own hit
identities (`sqlvalue_to_id_string`) and feeds them to `identity_in_predicate`
(`governed.rs:246`), which coerces each value via `coerce_filter` back to the
identity's declared logical type. If a mirror-returned identity cell fails to
coerce (data corruption / logical-type drift — unreachable with valid data and
not caller-forgeable; the caller supplies only type name, index name, and a
float vector), the resulting `QueryError::BadFilterValue` flows through the
total `query_error_response` mapping (`http.rs:425`, from #312) to a structured
400 echoing the "offending value" — as if the caller sent a bad filter — with
**no server-side log** (pre-#312: opaque 500 + `tracing::error!`). A
server-data-integrity fault masquerades as a caller fault and loses its
operator signal. #310 deliberately did not special-case it inside the mapping,
which would re-partialize the one total mapping.

Same class, same function: the `NoIdentity` construction at `handler.rs:610-614`
fires when a type with a served vector index has no declared identity — server
ontology/config state, also not caller-forgeable here — and renders 400 today.

## Design

Classify at the **source**, keeping `query_error_response` total and free of
path-sensitivity. Add one variant to `QueryError` (`handler.rs:106`):

```rust
/// A server-side fault detected while processing engine-derived (non-caller)
/// values — e.g. a mirror-returned identity cell that fails `coerce_filter`
/// against its declared logical type. Never caller-forgeable: renders as an
/// opaque 500, logged server-side with the wrapped error's full detail.
#[error("internal fault: {context}: {source}")]
Internal { context: &'static str, source: Box<QueryError> },
```

plus an `fn into_internal(self, context: &'static str) -> QueryError` helper
that boxes `self` into the variant.

**The seam:** the post-filter site in `vector_search` wraps every error born
from engine-derived inputs:
- `handler.rs:616`: `identity_in_predicate(...).map_err(|e| e.into_internal("vector-search post-filter: engine hit identity failed coercion"))?`.
  Only coercion errors can flow here — the `identity_governed()` guard at
  `:604` already returned `Forbidden` for the `BadFilter` arm.
- `handler.rs:610-614`: wrap the `NoIdentity` construction the same way.

Caller-value paths are untouched: `seed_predicates`/`coerce_visible_predicate`
(caller filters + `?_ids`), the object-set read at `handler.rs:848`, and the
cursor coercion at `handler.rs:464` (server-emitted but caller-echoed, hence
forgeable — `BadPagination` 400 stays correct) all keep their 400s.

Rendering — the new variant forces (by totality, no catch-all) one deliberate
arm in each wire mapping:
- `query_error_response` (`http.rs:425`): `QueryError::Internal { .. } => internal_error(context, e)`
  — the existing helper (`http.rs:59`) emits the one structured
  `tracing::error!(error = %e, ...)` (Display carries the wrapped
  column/expected/value detail) and the opaque `500 "internal error"` body.
- The Flight export `QueryError -> Status` mapping (`flight_export.rs:197`
  neighborhood): `tracing::error!` + opaque `Status::internal("internal error")`
  (its `BadFilterValue -> invalid_argument` arm stays — export filters are
  caller-supplied).

## Acceptance criteria

Red-first: each test lands failing before the fix.
1. A `vector_search` whose serving stub returns a hit identity cell that does
   not coerce to the declared identity logical type (e.g. `SqlValue::Text("x")`
   against an `Integer` identity, with a row filter present so the post-filter
   runs) yields **500** with the opaque `"internal error"` body — not 400, no
   `{error: "bad_filter_value"}` echo of engine data — and emits exactly one
   server-side `tracing::error!` carrying the context plus the coercion detail
   (column + expected type), asserted via a capturing subscriber.
2. A caller-supplied bad filter value (`?col=notanint` on an Integer column)
   still yields the structured **400** `bad_filter_value` body, and a caller
   object-set read (`?_ids=notanint`) still yields **400** — the
   caller-forgeable `identity_in_predicate` paths are unaffected.
3. `buck2 test //src/services/query-api/...` green; clippy/prek clean.

## Out of scope

- The lossy `{other:?}` Debug fallback in `sqlvalue_to_id_string` for
  non-Int/Text identities (a hit/survivor-matching correctness gap, not a
  fault-classification one) — separate item if pursued.
- Any change to `query_error_response`'s totality contract or to the 400
  classification of caller-echoed cursors (`BadPagination`).
- Other services (ingest/engine): the sweep found the class only in query-api.
