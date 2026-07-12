# Flight-export config seam Design

> **Status:** design (direction). This spec makes
> `road-flight-export-config-seam` build-ready (promoted 2026-07-12 from
> `#fut-flight-export-config-seam`). A separate work agent writes the
> implementation plan from it and builds it.

## Problem

The external Arrow Flight export listener is configured by two raw
`env.get`/`parse_var` reads inside `serve()`
(`query-api/src/serve.rs:99-106`): `LOOM_FLIGHT_BIND_ADDR` (opt-in bind
address; unset ⇒ listener off) and `LOOM_EXPORT_MAX_ROWS` (per-export row
cap, default `DEFAULT_EXPORT_MAX_ROWS = 1_000_000`, `serve.rs:16`). They
predate the typed `QueryApiConfig`/`LayeredConfig` seam (defaults < file <
env with validation, `loom-config/src/lib.rs:104-115`) that every other
query-api knob now rides — the SQL wire directly below them
(`serve.rs:108-120`) is fully typed via `SqlWireTuning`
(`query-api/src/config.rs:40-75`), and `config.rs:37-39` explicitly names
this item as the leftover. Consequence: no config-file layer, no startup
validation naming the key, and one inconsistent corner in the config
surface.

## Design

A verbatim sibling of `SqlWireTuning`:

```rust
/// query-api/src/config.rs
#[derive(Debug, Clone, Deserialize, PartialEq)]
#[serde(default, deny_unknown_fields)]
pub struct FlightExportTuning {
    /// None (default) = the export listener never starts.
    pub bind_addr: Option<String>,
    /// Per-export hard row cap.
    pub max_rows: u32,           // default 1_000_000
}
```

- `overlay_env` maps the **shipped env names unchanged** —
  `LOOM_FLIGHT_BIND_ADDR` via `overlay_opt`, `LOOM_EXPORT_MAX_ROWS` — so no
  deployment breaks; the knobs gain the file layer + validation.
- `validate`: `bind_addr` parses as `SocketAddr` (error naming the key);
  `max_rows == 0` rejected. Same shape as `SqlWireTuning::validate`
  (`config.rs:65-74`).
- `QueryApiConfig` gains `flight_export: FlightExportTuning`
  (`config.rs:81-100` delegation).
- `serve.rs:99-106` consumes `app_cfg.flight_export.bind_addr` /
  `.max_rows`; `DEFAULT_EXPORT_MAX_ROWS` moves to the tuning's `Default`
  impl; the raw reads are deleted. `spawn_flight_export` keeps its parsed
  `SocketAddr` re-parse or takes the validated value — implementer's choice,
  matching how the SQL wire consumes its addr (`serve.rs:108-120`).

Config-file authoring example (the new capability):

```json
{ "flight_export": { "bind_addr": "0.0.0.0:50052", "max_rows": 500000 } }
```

## Non-regression

- Env-only deployments behave identically (same keys, same defaults, same
  opt-in-by-presence contract).
- The error class improves deliberately: a malformed `LOOM_FLIGHT_BIND_ADDR`
  now fails at config load naming the key, instead of inside
  `spawn_flight_export` — same fail-loud startup, better message.
- `flight_export.rs` (the service + cap enforcement) is untouched.

## Testing

Mirror `query-api/tests/sql_wire_config.rs` case-for-case:

- Defaults: `bind_addr` None, `max_rows` 1_000_000.
- Env overlays both keys.
- File layer sets both; env overrides file.
- Malformed `bind_addr` → error naming `LOOM_FLIGHT_BIND_ADDR`.
- `max_rows: 0` → validation error.
- The existing flight-export e2e still boots the listener from env.

## Global constraints (loom-specific, carry into the plan)

- Strict clippy (pedantic + restriction); `#[expect(lint, reason)]` locally.
- `buck2 run //tools:prek -- run --all-files` before every commit.
- Tests are `rust_test` targets only.

## Out of scope (deferred)

- TLS/mTLS on the export wire — `#fut-flight-export-tls`.

## Acceptance

1. Both knobs load through `QueryApiConfig` (defaults < file < env,
   validated); the raw `env.get`/`parse_var` reads in `serve.rs` are gone.
2. Existing env-configured deployments and the export e2e behave
   identically.
3. Existing suites green (`buck2 test //src/...`).

## Interfaces (names the plan consumes)

- Consumes: `SqlWireTuning` as template (`query-api/src/config.rs:40-75`);
  `LayeredConfig`/`overlay_opt` (`loom-config/src/lib.rs:35-115`);
  `serve.rs:16,99-106,126-160`; `sql_wire_config.rs` test template.
- Produces: `FlightExportTuning` + `QueryApiConfig.flight_export`; the
  rewired `serve()` consumption; the config test file.
