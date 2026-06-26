# Error-handling debt (clippy `map_err_ignore` + panic-on-invariant)

**Resolved on PR #200 (branch `clippy-strict-lints`).** Every site below now carries
its source error, and all the tracked `#[expect(clippy::map_err_ignore)]` suppressions
have been deleted — `clippy::map_err_ignore` is enforced tree-wide with **zero**
production suppressions. This file is kept as the resolution log; see [`FUTURE.md`](FUTURE.md)
`#fut-clippy-map-err-debt`.

`clippy::map_err_ignore` is **enforced**: discarding the source error in `map_err(|_| …)`
loses debugging information. The adoption PR suppressed these with a tracked
`#[expect]`; the follow-up (this worklist) carried the error properly, the lint stopped
firing, and clippy forced each now-unfulfilled `#[expect]` to be removed.

---

## Group 1 — embed the source in the message (no error-type change) `{#err-1 status:fixed area:query-api,engine}`

Message-based errors: the source error is folded into the existing message string. Done
via a `bad_src(m, e)` helper in `filter.rs`/`params.rs` (`format!("{m}: {e}")`) and inline
`format!` at the engine/main sites.

- [x] `src/services/query-api/src/filter.rs` — `coerce_filter`: `json_repr_of` error (carries the unknown type name), number/int64 parse errors, ISO date/timestamp parse errors all folded into the `FilterError::BadValue` message.
- [x] `src/services/query-api/src/params.rs` — `parse_value`: same treatment as `filter.rs` (unknown logical type, int64, ISO date/timestamp).
- [x] `src/services/engine/src/service.rs` — `parse_id`: `Status::invalid_argument(format!("bad job id: {e}"))`.
- [x] `src/services/query-api/src/main.rs` — `LOOM_ENGINE_SOCKET` `VarError` folded into the boxed-error message.

## Group 2 — needs an error-type change `{#err-2 status:fixed area:query-api}`

`QueryError::BadFilter(String)` holds only a column name, so there was nowhere for the
source. Fix: added a sibling `BadFilterValue(#[from] crate::filter::FilterError)` variant
(`#[error(transparent)]`) — the `FilterError` already names the column, so the 6 coercion
sites collapse to a bare `?`. The three `http.rs` match arms render it into the 400 body
(`e.to_string()`) so the parse detail reaches the client; `BadFilter` stays for genuine
column-permission denials.

- [x] `src/services/query-api/src/handler.rs` — all 6 `coerce_filter`/`coerce_predicate` sites (`identity_in_predicate` + the read/chain/graph seed-predicate loops) now `?` into `BadFilterValue`.
- [x] `src/services/query-api/src/http.rs` — `BadFilterValue` arm added to all three `QueryError` match sites (400, body = source message).

## Group 3 — review: panic-on-invariant that could be a `Result` `{#err-3 status:fixed area:postgres,query-api}`

- [x] `src/control-plane/postgres/src/iceberg_mirror.rs` — `columns_of` converted to return `Result` (fixed earlier on this branch); both callers surface the error.
- [x] `src/services/query-api/src/serving.rs` — `one_cell`: the discarded `TryFromIntError` is folded into the integer-overflow message.

---

## Out of scope (NOT debt — listed so cascade agents don't "fix" them)

- `#[allow(clippy::too_many_arguments, reason = …)]` on builder/landing functions
  (`iceberg_landing.rs:57/142/289`, `ontology.rs:448`, `materialize.rs:57`,
  `typed.rs:27`) — legitimate; these args are structurally distinct and grouping
  them would hurt clarity. Leave as-is.
- The panic-safety lints in test/harness code — intentionally exempted (the
  `loom_rust_test` wrapper + the three harness `#![allow]`s). Not debt. The two
  harness `#![allow(clippy::map_err_ignore)]` (`testkit/src/lib.rs`,
  `postgres/src/fixture.rs`) are deliberate blanket carve-outs for test
  infrastructure, not production debt.
