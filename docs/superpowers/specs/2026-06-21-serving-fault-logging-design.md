# Serving-fault server-side logging — Design

> Closes `iss-serving-faults-not-logged`. `query-api` maps backend/serving faults
> to an opaque `500 "internal error"` — correct, a governance-fronted service must
> not echo SQL fragments or table/column names to the client — but it **drops the
> error entirely**, so an operator has nothing to diagnose with. The root cause is
> structural: the tracing pass (`road-tracing-spans`, done) deliberately deferred
> subscriber installation to "the binaries", so today there is **no subscriber
> anywhere** and every `#[tracing::instrument]` span across the control plane emits
> into a no-op. This slice installs the subscriber and logs the dropped faults,
> keeping the opaque client response byte-for-byte unchanged.

## Goal

Backend/serving faults that surface as an opaque 500 are **logged server-side with
full detail**, on both service binaries, without changing what the client sees.

## Background (why nothing is logged today)

The original tracing design (`2026-06-07-tracing-instrumentation-design.md`) wired
spans/events through the `tracing` facade in the libraries and **explicitly left
subscriber installation to the Step 3 services / binaries** ("the binaries install
the `Subscriber`"; "No subscriber/exporter/fmt setup anywhere — libraries only
emit"). That step was never taken: neither `query-api`'s nor `ingest`'s `main.rs`,
nor the shared `service_runtime` crate, installs a subscriber. So the control
plane's `#[tracing::instrument]` spans and any `error!`/`warn!` events go nowhere.
The `query-api` error mappers carry a standing `TODO(serving-tier): log e
server-side once a tracing subscriber is wired in the binary` to mark this gap.

## Part 1 — shared subscriber: `service_runtime::init_tracing()`

Install one subscriber, in shared runtime, used by both binaries.

- Add `pub fn init_tracing()` to `src/services/runtime/src/lib.rs`:

  ```rust
  pub fn init_tracing() {
      use tracing_subscriber::{fmt, EnvFilter};
      let filter = EnvFilter::try_from_default_env()
          .unwrap_or_else(|_| EnvFilter::new("info"));
      // try_init (not init) + .ok(): a second call (e.g. a test that also
      // installs a subscriber, or a re-entrant binary path) must not panic.
      let _ = fmt().with_env_filter(filter).try_init();
  }
  ```

  Env-driven via `RUST_LOG` (idiomatic), default level `info`, human-readable
  `fmt` output, idempotent.

- Call it as the **first statement** of both `main()`s:
  - `src/services/query-api/src/main.rs`
  - `src/services/ingest/src/main.rs`

  so spans/events are captured for the whole process lifetime, across both
  services (this is what finally gives the control-plane spans somewhere to go —
  not just the new query-api error logs).

- Dependency: `tracing-subscriber = { version = "0.3", features = ["env-filter"] }`
  on the **runtime** crate (the `env-filter` feature is required for `EnvFilter`;
  `fmt` is on by default). The `fmt` formatter needs no extra feature.

### Lockfile footgun (must-read for the work agent)

Adding `tracing-subscriber` changes `Cargo.lock`. Per the standing guard
(`CLAUDE.md` → *Third-party Rust deps*), a full re-resolve can silently **downgrade
`duckdb`** (loosely pinned `version = "1"`), which breaks every DuckLake serving
test at runtime with a catalog-version mismatch. The work agent must:

1. Use a **minimal-drift** lock update — add the dep and let only its subgraph
   move; do **not** run `cargo generate-lockfile`.
2. After `./tools/buckify.sh`, diff `Cargo.lock` against the merge-base for the
   native/`links` crates (`libduckdb-sys`, `zstd-sys`, `ring`) — they must not
   move. If `duckdb` drifted: `cargo update -p duckdb --precise 1.10503.1`
   (hermetic cargo via `eval "$(./tools/env.sh)"`), then re-buckify.
3. Run the **full** `buck2 test //src/...` before landing — a green per-crate
   build is not sufficient to catch a shared-dep regression.

## Part 2 — log the dropped faults: `query-api` `http.rs`

A single shared helper replaces the four near-identical opaque-500 arms (avoids the
verbatim duplication the `loom-duplication` routine flags):

```rust
use std::fmt::Display;

/// Log a backend/serving fault server-side, then return the opaque 500 the client
/// sees. The detail (`error = %e`) is for operators only — the response body
/// carries no internal detail (SQL fragments, table/column names).
fn internal_error(context: &str, e: impl Display) -> axum::response::Response {
    tracing::error!(error = %e, "{context}");
    (StatusCode::INTERNAL_SERVER_ERROR, "internal error").into_response()
}
```

Apply at the four sites, **binding the previously-discarded error**:

- `read_object` (~`http.rs:96`): `Err(_) => …` becomes
  `Err(e) => internal_error("object read serving fault", e)`.
- `chain_error` (~`http.rs:259`, takes `e: QueryError`): the `_ => …` catch-all
  becomes `other => internal_error("chain/association read serving fault", other)`.
- `graph_error` (~`http.rs:458`, takes `e: QueryError`): `_ => …` becomes
  `other => internal_error("graph read serving fault", other)`.
- `post_action` (~`http.rs:611`): `Err(_) => …` becomes
  `Err(e) => internal_error("action serving fault", e)`.

The caught errors are exactly the transparent backend variants —
`QueryError::{ControlPlane, Serving, Malformed}` and `ActionError::Serving` — whose
`Display` (`#[error(transparent)]`) yields the underlying detail, which is what
`%e` logs. The **client body stays exactly `"internal error"`** at all four sites.
Remove the now-obsolete `TODO(serving-tier)` comment block at the `read_object`
site.

`query-api`'s lib already depends on `//third-party:tracing`, so no new lib dep for
the `tracing::error!` macro.

## What this does NOT change

- **Which** responses are 500s, or any status code / body. Only the previously-500
  paths gain a log line; their bodies are unchanged.
- The opaque-body policy stays — no internal detail is added to any client
  response.
- The misconfigured-action arm (`ActionError::Misconfigured` → 500 **with** detail)
  is intentionally distinct and untouched: it is an operator-facing config message,
  not a backend fault, and is already surfaced.
- No change to the control-plane spans themselves — they were already emitted;
  they simply now have a subscriber.

## Testing

All tests are `rust_test` integration targets (no inline `#[cfg(test)]`).

- **Drop-site logging** (`query-api` `tests/serving_fault_logging.rs`, hermetic —
  no fixture): `#[traced_test]` from `//third-party:tracing-test` (the same
  `no-env-filter` setup the `worker` crate uses in `tests/worker.rs`). Build a
  synthetic backend error `QueryError::Serving(ServingError::Engine("boom".into()))`
  and assert, for `internal_error`, `chain_error`, and `graph_error`:
  - the response status is `500` and the body is exactly `"internal error"`
    (**no leak** — `"boom"` is *not* in the body), and
  - `logs_contain("boom")` (the detail **is** logged server-side).

  This pins both halves of the contract in one test. (`read_object` and
  `post_action` are async handlers that need backends to drive directly; the shared
  `internal_error` helper they call is covered above, and `chain_error`/
  `graph_error` are pure mappers exercised directly — together these cover the
  logging behavior without standing up a serving engine.)

- **`init_tracing` smoke** (`runtime` `tests/init_tracing.rs`): call
  `service_runtime::init_tracing()` twice and assert it does not panic (the
  idempotency the `try_init().ok()` guarantees). Does **not** use `#[traced_test]`
  (which installs its own subscriber) — it exercises the real init path.

- **Existing query-api read/action tests** stay green unchanged — the 500 bodies
  they may assert are byte-for-byte the same.

## Out of scope (noted, not built)

- **Structured/JSON log output** (`fmt().json()`), log rotation, and any non-`fmt`
  formatter — a deployment concern, deferred (YAGNI).
- **OpenTelemetry / OTLP exporter** — span export to a collector; deferred.
- **The `metrics` crate** (counters/histograms) — already tracked as
  `fut-metrics-crate`; this slice installs a tracing subscriber only.
- **Changing fault classification** — which errors map to 500 vs 4xx is unchanged.

## Files

- Modify: `src/services/runtime/src/lib.rs` — add `init_tracing()`.
- Modify: `src/services/runtime/Cargo.toml` — add `tracing-subscriber`
  (`env-filter` feature); `src/services/runtime/BUCK` — add
  `//third-party:tracing-subscriber` to `:runtime` deps and a smoke `rust_test`
  target for `tests/init_tracing.rs`.
- Modify: `src/services/query-api/src/main.rs`, `src/services/ingest/src/main.rs` —
  call `service_runtime::init_tracing()` first in `main()`.
- Modify: `src/services/query-api/src/http.rs` — add the `internal_error` helper;
  bind + log at the four sites; remove the `TODO(serving-tier)` comment.
- Create: `src/services/query-api/tests/serving_fault_logging.rs` and its
  `rust_test` `BUCK` target (deps incl. `//third-party:tracing-test`,
  `//third-party:axum`).
- Regenerate: `third-party/BUCK` (via `./tools/buckify.sh`) and `Cargo.lock`
  (minimal-drift — see the lockfile footgun above).
- Modify: `docs/ISSUES.md` — close `iss-serving-faults-not-logged`
  (`[x] status:fixed pr:#<n>`), repoint its `spec:` to this design.
- Core (`src/control-plane/core/`) is **untouched**.
