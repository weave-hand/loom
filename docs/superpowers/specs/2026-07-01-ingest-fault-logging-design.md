# Ingest handler fault logging — server-side diagnosability for opaque 500s

- **Date:** 2026-07-01
- **Area:** ingest
- **Closes:** `iss-ingest-model-500-unlogged`
- **Mirror of:** `iss-serving-faults-not-logged` (query-api, PR #140, spec `2026-06-21-serving-fault-logging-design`)

## Problem

`ingest/src/http.rs` returns an opaque `(StatusCode::INTERNAL_SERVER_ERROR, "internal error")`
at **10 arms across three handlers**, none of which logs the underlying fault. When one
fires, an operator has nothing to diagnose from — no SQL fragment, no table/column name, no
error variant. This is the ingest twin of the query-api gap already closed by
`iss-serving-faults-not-logged`: query-api added a `internal_error(context, e)` helper that
logs the detail server-side via `tracing::error!` while returning the opaque body unchanged,
and applied it at every opaque-500 arm.

The issue register flags the infer-and-create `define_type` arm as the highest-value one to
instrument (a brand-new write to `ontology.object_type` under contention is the path most
likely to hit a transient fault), but the fix covers **all** opaque-500 arms in the file so the
handler's diagnosability is uniform and no follow-up issue is needed for the siblings.

This is a pure observability change: response status and body are unchanged, byte for byte.

## Approach

### 1. Add a local `internal_error` helper

Add a private helper to `ingest/src/http.rs`, copied verbatim from `query-api/src/http.rs:54`:

```rust
/// Log a backend/serving fault server-side, then return the opaque 500 the client
/// sees. The detail (`error = %e`) is for operators only — the response body
/// carries no internal detail (SQL fragments, table/column names).
fn internal_error(context: &str, e: impl Display) -> axum::response::Response {
    tracing::error!(error = %e, "{context}");
    (StatusCode::INTERNAL_SERVER_ERROR, "internal error").into_response()
}
```

**Decision — local copy, not a shared hoist.** The helper is duplicated (~6 lines) rather than
extracted into `service_runtime` (a shared dep of both binaries). This keeps the change a
faithful, minimal mirror of the shipped reference fix and does not touch query-api's shipped
code. The duplication is deliberate and acceptable; if the duplication routine later flags the
pair, hoisting to `service_runtime` is a separate, considered call — out of scope here.

### 2. Replace every opaque-500 arm

Swap all 10 arms to `internal_error("<context>", e)`, binding the currently-discarded error
(`Err(e) =>` in place of `Err(_) =>`). The two `land_model` sites that use `.is_err()` are
restructured to capture the error (`if let Err(e) = … { return internal_error(…, e); }`).

| Handler | Site | Context label |
|---|---|---|
| `compact` | `serde_json::to_value(CompactJob)` fault | `"compact job serialize"` |
| `compact` | `queue().enqueue` fault | `"enqueue compact job"` |
| `land_model` | acl `check` `Err` | `"model acl check"` |
| `land_model` | `define_type` failure (`.is_err()` → `if let Err(e)`) | `"model define_type"` |
| `land_model` | re-resolve `get_type` `Err` | `"model re-resolve type"` |
| `land_model` | outer `get_type` catch-all `Err` | `"model resolve type"` |
| `land_model` | `resolve_columns` catch-all `Err` | `"model resolve columns"` |
| `land_model` | `materializer.land` catch-all `Err` | `"model land"` |
| `land` | `resolve_columns` catch-all `Err` | `"land resolve columns"` |
| `land` | `materializer.land` catch-all `Err` | `"land"` |

The context strings are operator-facing log messages only; they never reach the client.

### 3. Tracing initialization — no change

The ingest binary already installs the subscriber (`service_runtime::init_tracing()` in
`ingest/src/main.rs`), so no binary change is required — only the helper and the call sites.

## Testing

Add a new **pure-logic** `rust_test` at `src/services/ingest/tests/serving_fault_logging.rs`
(no Postgres fixture, so it runs on remote execution — a bare `rust_test`, not
`loom_fixture_test`), modeled on `query-api/tests/serving_fault_logging.rs`:

- A stub `LandingMaterializer` whose `land` returns a backend `Err` drives a `materializer.land`
  catch-all arm; a `MemoryControlPlane` supplies the control plane with a `Write` grant on the
  target type so the request reaches the land call.
- `#[tokio::test]` + `#[traced_test]`: drive the route with `oneshot`, then assert
  1. status is `500`,
  2. body is exactly `"internal error"` and does **not** contain the injected fault detail,
  3. `logs_contain(<fault detail>)` — the detail was logged server-side.

One representative arm pins the helper's behavior, matching the reference test's scope (it
pinned one of four arms, not all). Wire the test target in `src/services/ingest/BUCK` with
`//third-party:tracing-test` added to its `deps` (as query-api's target does).

## Non-goals

- **No `service_runtime` hoist** — the helper stays a local copy (see decision above).
- **No status/body changes** — the client-visible contract is untouched; this is purely
  server-side observability.
- **No change to the sibling `land`/`compact` semantics** beyond logging — the same faults
  still return the same opaque 500.
