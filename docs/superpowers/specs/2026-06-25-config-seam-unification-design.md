# Design: configuration seam unification

> **Status:** approved design (2026-06-25). Promotes the FUTURE idea
> [[fut-config-deploy-ergonomics]] into a committed slice. Consolidates loom's
> scattered operational tuning knobs onto one typed, validated config seam loadable
> **both from a mounted config file (a Kubernetes ConfigMap) and from environment
> variables**, so a deployment can see and set them, and a bad value fails startup
> instead of silently falling back. **No behavior change at default values.** Tracked
> as `road-config-seam-unification`.

## Problem

Operational tuning values live in three incompatible places:

1. **The typed seam** — `service_runtime::Config` (`from_env` → `from_map`) parses
   `LOOM_BIND_ADDR`, the `LOOM_DB_*` set, `LOOM_DATA_PATH`, `LOOM_WAREHOUSE_URI`,
   `LOOM_LOCK_TIMEOUT_MS`, and the `AWS_*` S3 vars, with typed errors via `ConfigError`.
2. **Ad-hoc `std::env::var` reads in each `main.rs`** — `LOOM_INLINE_BYTE_LIMIT` and
   `LOOM_FLUSH_BYTE_THRESHOLD` (ingest + query-api), the `LOOM_*_BACKEND` selectors,
   `LOOM_ENGINE_SOCKET`, `LOOM_WORKER_ID`. These bypass the typed seam and parse
   **lossily**: `src/services/ingest/src/main.rs` reads the byte limits with
   `.ok().and_then(|s| s.parse().ok())`, so `LOOM_INLINE_BYTE_LIMIT=abc` silently
   falls back to the default instead of failing — a real (if minor) defect.
3. **Pure hardcoded consts** — `datafusion_io`'s Parquet write defaults
   (`target_file_size_bytes = 128 MiB`, `max_files = 64`, `compression_factor = 0.3`),
   the worker poll interval (`5s`) and exponential-backoff ceiling/cap, and query-api's
   default page limit (`1000`). No env path at all.

The consequence: deployment can't reliably tune what it can't see, there's no single
contract of "what is configurable," and the one env path that exists swallows bad input.

## Goal

One typed, validated config contract for **operational tuning** knobs: loadable from a
structured **config file** (a ConfigMap mounted as a file) and/or from **environment
variables**, parsed once at startup, co-located with the code each knob tunes, with a real
`ConfigError` on bad input. Defaults are unchanged, so a deployment that sets nothing
behaves exactly as today.

This is the **code seam + config/env contract** only. Threading the new knobs through the
Helm chart `values.yaml` (rendering the ConfigMap) is explicitly **out of scope** and stays
with [[fut-deploy-followups]].

## Scope: what becomes config vs. what stays `const`

The inventory splits along a deliberate line, recorded here so future readers know the rule:
**operational knobs an operator legitimately tunes per-deployment become config; safety
guardrails and domain invariants stay `const`.**

### Promoted to config (operational tuning)

| Domain | Knob | Default | Env var | Notes |
|---|---|---|---|---|
| Routing (ingest, query-api) | `inline_byte_limit` | 16 MiB | `LOOM_INLINE_BYTE_LIMIT` | already env-read, but lossily |
| Routing (ingest, query-api) | `flush_byte_threshold` | 64 MiB | `LOOM_FLUSH_BYTE_THRESHOLD` | already env-read, but lossily |
| Write (datafusion-io) | `target_file_size_bytes` | 128 MiB | `LOOM_WRITE_TARGET_FILE_BYTES` | today a `Default` impl, no env path |
| Write (datafusion-io) | `max_files` | 64 | `LOOM_WRITE_MAX_FILES` | |
| Write (datafusion-io) | `compression_factor` | 0.3 | `LOOM_WRITE_COMPRESSION_FACTOR` | |
| Worker (worker, transform) | `poll_interval` | 5s | `LOOM_WORKER_POLL_INTERVAL_MS` | notification-miss fallback poll |
| Worker (worker, transform) | `backoff_ceiling` | 60s | `LOOM_WORKER_BACKOFF_CEILING_MS` | exp-backoff cap |
| Worker (worker, transform) | `backoff_max_attempts` | 6 | `LOOM_WORKER_BACKOFF_MAX_ATTEMPTS` | exp-backoff iteration cap |
| Serving (query-api) | `default_limit` | 1000 | `LOOM_SERVING_DEFAULT_LIMIT` | default page size |
| DB pool (service_runtime) | `max_connections` | sqlx default | `LOOM_DB_MAX_CONNECTIONS` | extends `DbConfig` |

(Exact env-var names are the design's proposal; the implementation plan may adjust
spelling for consistency, but the `LOOM_`-prefixed, unit-suffixed convention is fixed.)

### Stays `const` (not config)

- **Safety guardrails** — `MAX_CHAIN_DEPTH` (4), `MAX_GRAPH_DEPTH` (10),
  `DEFAULT_GRAPH_DEPTH` (5). These bound blast radius / recursion; an operator should not
  be able to lift them per-deployment. `DEFAULT_GRAPH_DEPTH` stays const because it is
  coupled to the `MAX_GRAPH_DEPTH` guardrail, not a deployment concern.
- **Domain invariants** — `LOOM_DATASET_NAMESPACE` (`"loom"`), `LOOM_TYPE_NAMESPACE`
  (`"loom:type"`), `LOOM_STORE_URL` (`"loom://data"`), `FLUSH_JOB_KIND`
  (`"flush_table"`). These are identity/protocol constants, not tunables.
- **Derived buffers** — e.g. the engine-wire client's `+2s` deadline cushion and the
  worker heartbeat (`lease / 3`) are derived from other values, not independent knobs.

Backend selectors (`LOOM_LANDING_BACKEND`, `LOOM_SERVING_BACKEND`,
`LOOM_TRANSFORM_BACKEND`), `LOOM_ENGINE_SOCKET`, and `LOOM_WORKER_ID` are already
env-driven and are **not reshaped** by this slice; folding them into the typed structs is
a tidy-up the plan may include but is not required for the slice to be complete.

## Structure: per-domain typed sub-structs

Each domain owns a small typed config struct, co-located with the code it tunes, rather
than growing `service_runtime::Config` into a god-object that must know every service's
internals (and would force `datafusion-io` ↔ `runtime` dependency cycles). Each struct:

- holds the knobs for one concern,
- has a `Default` impl encoding today's constants (so defaults are unchanged),
- derives `serde::{Serialize, Deserialize}` with **field-level `#[serde(default)]`** so a
  partial config file (any subset of keys) deserializes, missing fields falling to the
  `Default`,
- has an env-overlay step (`overlay_env(&mut self, &HashMap<String, String>) ->
  Result<(), ConfigError>`) that applies any present `LOOM_*` var on top of the current
  value,
- is unit-tested in isolation.

Proposed homes:

- **`datafusion_io::WriteTuning`** — the existing `Default`-impl write-config struct gains
  the serde derives + `overlay_env` + validation (it already carries
  `target_file_size_bytes`/`max_files`/`compression_factor`).
- **`RoutingTuning { inline_byte_limit, flush_byte_threshold }`** — shared by ingest and
  query-api; lives with the flush/landing primitive both consume rather than being
  re-read in each `main.rs`.
- **`WorkerTuning { poll_interval, backoff_ceiling, backoff_max_attempts }`** — in the
  worker domain; the transform handler (which mirrors the same backoff formula) consumes
  the same struct so the two backoff sites stop drifting.
- **`ServingTuning { default_limit }`** — query-api.
- **`max_connections`** — extends the existing `DbConfig` inside `service_runtime`
  (it already owns the pool construction), not a new struct.

## Loading model: defaults < file < env

Config resolves as three overlaid layers, lowest precedence first — each layer overrides
only the keys it sets, so an operator can mix a structured ConfigMap file with per-pod
env/Secret overrides:

1. **Defaults** — each struct's `Default` (today's constants).
2. **Config file** — if `LOOM_CONFIG_FILE` points at a path, the structured document there
   is deserialized over the defaults. This is the file a ConfigMap is mounted as. Absent
   ⇒ this layer is skipped. Format is **JSON** via the already-vendored `serde_json` (a
   ConfigMap value holds the JSON document under one key); see *Format* below.
3. **Environment variables** — the flat `LOOM_*` vars from the scope table overlay the
   result last (env wins), so secrets and per-pod tuning need no file edit.

### Per-binary composition (no god-aggregate)

Each binary owns a thin top-level config struct composing only the sub-structs it needs
(e.g. ingest: routing + write; query-api: routing + write + serving; worker: worker
tuning), itself `Deserialize` with `#[serde(default)]`. The file's top-level keys are those
domains. There is deliberately **no single global aggregate** spanning all crates — that
would force `service_runtime` to depend on every domain crate and risk dependency cycles
(`datafusion-io` ↔ `runtime`). The per-domain structs are the reusable serde building
blocks; each binary's struct is the composition.

The load sequence in each `main`: read the env **once** into a `HashMap` (a
`service_runtime::env_map()` helper = `std::env::vars().collect()`), build the binary's
config struct from `Default`, deserialize the file over it when `LOOM_CONFIG_FILE` is set,
then `overlay_env(&env)` each sub-struct. One snapshot keeps loading consistent and
testable without touching the real environment or filesystem (tests pass a map + a string).

### Typed env overlay

`service_runtime` gains a small reusable typed-parse helper alongside the existing
`ConfigError`/`invalid` machinery, used by every `overlay_env`:

```rust
// applies vars[key] over `slot` if present; ConfigError if present-but-invalid; no-op if absent
fn overlay_opt<T: FromStr>(vars: &HashMap<String, String>, key: &str, slot: &mut T) -> Result<(), ConfigError>
```

so the env layer is consistent across domains and **a present-but-malformed value is a
startup `ConfigError`, not a silent fallback** — the fix for today's lossy `.ok()` reads.
Validation beyond parse (e.g. `compression_factor` in `(0, 1]`, `max_files >= 1`) runs
after both file and env layers are applied and surfaces as `ConfigError` naming the
offending key, so an out-of-range value from *either* source fails startup.

### Format

JSON, via `serde_json` (already vendored — no new dependency). A ConfigMap mounted as a
file carries the JSON document; JSON is a strict subset of YAML, so adding YAML authoring
later is purely additive. YAML is **not** added now: the de-facto crate `serde_yaml` is
archived upstream, so pulling a maintained YAML crate is its own decision, deferred to
[[fut-config-yaml-format]].

## Error handling

All parse/validation failures funnel through `ConfigError` (the existing type), which each
`main` already surfaces as a startup error. The net effect is fail-fast on misconfiguration:
a bad `LOOM_*` value aborts boot with a named, typed error instead of booting with a
silently-wrong default.

## Testing

Each tuning struct gets a sibling `tests/<name>_config.rs` `rust_test` (mirroring the
existing `src/services/runtime/tests/config.rs`), covering:

- **defaults** — empty map + no file yields today's constants,
- **from file** — a JSON document (full and partial) deserializes; omitted keys fall to
  `Default`,
- **env overlay** — a set `LOOM_*` var is parsed and applied over the default/file value,
- **precedence** — file sets a key, env overrides the *same* key ⇒ env wins; file sets a
  key the env leaves alone ⇒ file value survives,
- **invalid value** — a present-but-unparseable/out-of-range value from *either* the file
  or an env var yields `ConfigError` naming the key (the regression guard for the
  lossy-parse fix).

These are pure tests over a `HashMap` + a JSON string — no Postgres/DuckDB fixtures — so
they run on remote execution, not pinned local. The `overlay_opt` helper gets its own
focused test in `service_runtime`.

## Boundaries / out of scope

- **Helm/deploy wiring** — surfacing these knobs in the chart `values.yaml`, rendering the
  ConfigMap, and mounting it / setting `LOOM_CONFIG_FILE` on the `Deployment` stays with
  [[fut-deploy-followups]]. This slice ends at the code seam + the documented contract (the
  table above) + file loading; it does not touch the chart.
- **YAML config format** — only JSON is supported now (`serde_json`); a maintained YAML
  crate is deferred to [[fut-config-yaml-format]].
- **Guardrails and domain invariants** — explicitly *not* made configurable (see scope).
- **Backend selectors / socket / worker-id** — already env-driven; reshaping them into the
  typed structs is optional tidy-up, not a slice requirement.
- **Connection-pool tuning beyond `max_connections`** (idle timeout, acquire timeout) —
  deferred until there's a concrete need; only `max_connections` is promoted now.

## Definition of done

- The knobs in the scope table are read through typed per-domain structs (`Default` +
  serde `Serialize`/`Deserialize` + `overlay_env`), composed per-binary and resolved as
  defaults < file < env from a single env snapshot in each `main`.
- A structured JSON config file at `LOOM_CONFIG_FILE` deserializes into the binary's config
  (partial docs allowed); env vars override individual keys on top.
- The lossy `.ok()` byte-limit reads are gone; a malformed value from file or env fails
  startup with a `ConfigError` naming the key.
- Per-struct `rust_test`s cover defaults / from-file / env-overlay / precedence / invalid
  for each new struct, plus an `overlay_opt` test, all green under `buck2 test //src/...`.
- The guardrail/invariant `const`s are unchanged and documented as deliberately non-config.
- No default-value behavior change anywhere.
