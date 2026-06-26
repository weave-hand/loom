# Error-handling debt (clippy `map_err_ignore` + panic-on-invariant)

This is the worklist for the **cascade PRs** that fix the error handling papered
over during clippy adoption (branch `clippy-strict-lints`, the
`2026-06-26-stricter-clippy-config` plan).

`clippy::map_err_ignore` is **enforced** (not allowlisted): discarding the source
error in `map_err(|_| …)` loses debugging information. The adoption PR did NOT
genuinely fix these sites — it suppressed them with a tracked
`#[expect(clippy::map_err_ignore, reason = "error-handling debt — see docs/error-handling-debt.md")]`
so the gate stays green and the lint stays on. `#[expect]` **ratchets**: when a
cascade PR carries the error properly, the lint stops firing, the expectation goes
unfulfilled, and clippy forces the `#[expect]` to be removed. So "fix the site,
delete its `#[expect]`" is the loop.

Each cascade PR should be one **Group** below (independently reviewable, green on
landing). Line numbers are as of branch `clippy-strict-lints` HEAD and will drift —
the function names are the stable anchor.

---

## Group 1 — embed the source in the message (no error-type change) `{#err-1 status:open area:query-api,engine}`

These errors are message-based, so the source error can be folded into the existing
message string. Mechanical and high-value (better user-facing diagnostics).

`FilterError::BadValue(field, message)` — fix by threading the source into `message`,
e.g. change `let bad = |m: &str| FilterError::BadValue(name.to_string(), m.to_string())`
callers to `|m, e| … format!("{m}: {e}")`, or add a `bad_src(m, e)` helper.

- [ ] `src/services/query-api/src/filter.rs:32` — `coerce_filter`: `json_repr_of(logical_ty).map_err(|_| bad("unknown logical type"))` → carry the `json_repr_of` error.
- [ ] `src/services/query-api/src/filter.rs:40` — `coerce_filter`: number `.parse().map_err(|_| bad("expected a number"))` → include the `ParseFloatError`/`ParseIntError`.
- [ ] `src/services/query-api/src/filter.rs:46` — `coerce_filter`: `.map_err(|_| bad("not an int64"))`.
- [ ] `src/services/query-api/src/filter.rs:57` — `coerce_filter`: `.map_err(|_| bad("invalid ISO date"))` → include the `time` parse error.
- [ ] `src/services/query-api/src/filter.rs:64` — `coerce_filter`: `.map_err(|_| bad("invalid ISO timestamp"))`.
- [ ] `src/services/query-api/src/params.rs:50` — `coerce_param` (mirror of filter.rs:32).
- [ ] `src/services/query-api/src/params.rs:67` — `coerce_param`: `.map_err(|_| bad("not an int64"))`.
- [ ] `src/services/query-api/src/params.rs:84` — `coerce_param`: `.map_err(|_| bad("invalid ISO date"))`.
- [ ] `src/services/query-api/src/params.rs:94` — `coerce_param`: `.map_err(|_| bad("invalid ISO timestamp"))`.
- [ ] `src/services/engine/src/service.rs:27` — `.map_err(|_| Status::invalid_argument("bad job id"))` → `Status::invalid_argument(format!("bad job id: {e}"))`.
- [ ] `src/services/query-api/src/main.rs:48` — `std::env::var("LOOM_ENGINE_SOCKET").map_err(|_| -> Box<dyn Error> …)` → carry the `VarError` (it is already a `std::error::Error`).

## Group 2 — needs an error-type change `{#err-2 status:open area:query-api}`

`QueryError::BadFilter(String)` holds only a column name, so there is nowhere to put
the source error. Fix: add a source to the variant — e.g.
`BadFilter { column: String, #[source] source: Option<Box<dyn Error + Send + Sync>> }`
(or a sibling `BadFilterValue` variant) — then update the `http.rs` match arms
(`handler.rs:43` enum def; arms at `http.rs:119/274/464`) and carry the error at:

- [ ] `src/services/query-api/src/handler.rs:154` — `.map_err(|_| QueryError::BadFilter(identity.clone()))`.
- [ ] `src/services/query-api/src/handler.rs:230` — `.map_err(|_| QueryError::BadFilter(col.clone()))`.
- [ ] `src/services/query-api/src/handler.rs:614` — `.map_err(|_| QueryError::BadFilter(f.column.clone()))`.
- [ ] `src/services/query-api/src/handler.rs:885` — `.map_err(|_| QueryError::BadFilter(col.clone()))`.
- [ ] `src/services/query-api/src/handler.rs:1015` — `.map_err(|_| QueryError::BadFilter(col.clone()))`.
- [ ] `src/services/query-api/src/handler.rs:1207` — `.map_err(|_| QueryError::BadFilter(col.clone()))`.

## Group 3 — review: panic-on-invariant that could be a `Result` `{#err-3 status:open area:postgres,query-api}`

Not `map_err` — these are functions that `panic!`/`unreachable!` on an invariant
rather than returning a `Result`. They are currently suppressed with a reasoned
`#[allow(clippy::panic, …)]`. Decide per site: genuinely-unrecoverable invariant
(keep the `#[allow]`, it is honest) vs. should-propagate (refactor to `Result`).

- [ ] `src/control-plane/postgres/src/iceberg_mirror.rs:285` — `columns_of` returns `Vec`, `panic!`s on "missing vector doc" / "unsupported column type". If callers could surface these as errors, change the signature to `Result`. Currently `#[allow(clippy::panic)]`.
- [ ] `src/services/query-api/src/serving.rs:110` — `(*i).try_into().map_err(|_| …)` numeric narrowing; confirm the discarded `TryFromIntError` adds nothing, else fold it into Group 1.

---

## Out of scope (NOT debt — listed so cascade agents don't "fix" them)

- `#[allow(clippy::too_many_arguments, reason = …)]` on builder/landing functions
  (`iceberg_landing.rs:57/142/289`, `ontology.rs:448`, `materialize.rs:57`,
  `typed.rs:27`) — legitimate; these args are structurally distinct and grouping
  them would hurt clarity. Leave as-is.
- The panic-safety lints in test/harness code — intentionally exempted (the
  `loom_rust_test` wrapper + the three harness `#![allow]`s). Not debt.
