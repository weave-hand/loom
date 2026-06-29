# Apply `LOOM_WRITE_*` to transform output (thread WriteConfig through run_transform)

- **Date:** 2026-06-29
- **Area:** transform
- **Register items:** promotes [[fut-transform-write-tuning]] → [[road-transform-write-tuning]]
- **Status:** spec (ready for a work agent to plan + build)

## Problem

[[road-config-seam-unification]] made `datafusion_io::WriteConfig` (target file size /
max files / compression factor) file+env-configurable and threaded it into ingest's
`materialize::land` and the worker compaction path. The transform binary was left out:
`run_transform` (`src/services/transform/src/run.rs:182`) writes its output with a
**hardcoded default**:

```rust
let written = write_dataset(store, &dir_prefix, schema, &batches, &WriteConfig::default()).await?;
```

So an operator who sets `LOOM_WRITE_TARGET_FILE_BYTES` / `LOOM_WRITE_MAX_FILES` /
`LOOM_WRITE_COMPRESSION_FACTOR` sees those knobs apply to ingest landing and worker
compaction, but **transform output silently ignores them** — derived datasets are always
written with default file sizing. This is a latent config-ignore inconsistency: the seam
slice's promise is "one place to tune writes," and transform is the hole in it.

Two precise notes from reading the code:

- `compact.rs` is **already correct** — `compact_table` takes a `CompactConfig { write:
  WriteConfig }` and uses `cfg.write` (`compact.rs:84`). Only the **`run_transform` path**
  hardcodes the default. (Compaction runs in the worker binary, which the seam slice
  already wired.)
- `TransformConfig` (`transform/src/main.rs:22`) currently carries **only**
  `worker: WorkerTuning` — it does **not** yet compose a `WriteConfig`. So this is not
  merely "pass an already-composed value down": the binary must also start composing
  `WriteConfig` from the seam. (The FUTURE note's "composed and validated at transform
  startup" was slightly optimistic; this spec closes that too.)

## Design

Mirror `IngestConfig` (`src/services/ingest/src/config.rs`), which already does exactly
this — composes `write: datafusion_io::WriteConfig` and chains `overlay_env`/`validate`
into it.

### 1. Compose `WriteConfig` in the transform binary

Add a `write: datafusion_io::WriteConfig` field to `TransformConfig`
(`transform/src/main.rs`), and chain it in the `LayeredConfig` impl:

```rust
struct TransformConfig {
    worker: loom_config::WorkerTuning,
    write: datafusion_io::WriteConfig,
}
// overlay_env: self.worker.overlay_env(env)?; self.write.overlay_env(env)?;
// validate:    self.worker.validate()?;      self.write.validate()?;
```

`service_runtime::load` then composes it defaults < file < env, and a malformed
`LOOM_WRITE_*` fails transform startup like every other tuned binary (validation already
lives in `WriteConfig::validate`).

### 2. Thread `&WriteConfig` to the write call

Add a `write: &WriteConfig` parameter to:

- `run_transform` (`run.rs`) — pass it to `write_dataset` at line 182, replacing
  `&WriteConfig::default()`.
- `transform_handler` and `typed_transform_handler` (`handler.rs`) — forward it to
  `run_transform`.

In `main.rs`, capture `tcfg.write` into the worker closure (alongside `store`/`root_url`)
and pass `&write` to both handler calls.

### 3. Ripple to test call sites

The transform fixture tests that call the handlers / `run_transform` gain the new
argument; pass `&WriteConfig::default()` where a test does not exercise tuning (a pure
mechanical addition), and a real `WriteConfig` in the new tuning test below.

## Scope

In scope:

- `TransformConfig` gains `write: WriteConfig` (overlay + validate chained).
- `&WriteConfig` parameter threaded through `transform_handler` /
  `typed_transform_handler` / `run_transform`; the `run.rs:182` default removed.
- `main.rs` passes the composed `tcfg.write`.
- Test call-site ripple + the tuning test below.

Out of scope:

- The compaction path (`compact_table` / `CompactConfig`) — already threads `cfg.write`.
- [[fut-transform-backoff-unify]] (the retry-backoff unification — a separate behavior
  change), and any new `LOOM_WRITE_*` knob (the existing three are sufficient).
- Changing `WriteConfig`'s own defaults — behavior is unchanged when no `LOOM_WRITE_*`
  is set (composed value == `WriteConfig::default()`), so this is **not** a default-value
  behavior change.

## Testing

A transform fixture test (`loom_fixture_test`, the existing
`transform_e2e.rs`/`transform_e2e_support.rs` harness — `rust_test` integration target,
never inline `#[cfg(test)]`) proving the knob now reaches transform output:

1. Run a transform producing enough rows to span multiple target files, with a
   **non-default** `WriteConfig` (e.g. a small `target_file_bytes`, or a `max_files`
   cap) threaded in.
2. Assert the committed output file **count** reflects the tuned config (the
   size-estimated repartition in `write_dataset` produces the expected N files) — and
   that the same transform under `WriteConfig::default()` produces a different,
   default-sized layout.

This is the observable that was silently fixed at the default before. (A unit assertion
that `TransformConfig` overlays `LOOM_WRITE_*` is optional — the composition is the same
`WriteConfig::overlay_env` already tested for ingest; the integration test is the one
that proves the value reaches `write_dataset`.)

## Risk

- Behavior-preserving by default: with no `LOOM_WRITE_*` set, the threaded value equals
  the previous hardcoded `WriteConfig::default()`, so existing transforms write
  byte-identically.
- The change is mechanical (one composed field + one threaded parameter); the only
  breadth is the test call-site ripple, which is additive (`&WriteConfig::default()`).
- The pattern is copied verbatim from the proven `IngestConfig`, so the seam composition
  carries no new design risk.
