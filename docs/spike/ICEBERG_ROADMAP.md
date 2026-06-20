# Iceberg Adapter — Roadmap

> Status snapshot as of 2026-06-17. Captures what the Iceberg table-format
> adapter is, what's shipped, and what's deferred. This is a spike/roadmap
> note, not a spec — each remaining slice gets its own
> `docs/superpowers/specs/` design before implementation.

## What it is

An Iceberg table-format adapter that **coexists** with DuckLake. It serves
loom's `core::Catalog` from a loom-owned `iceberg_mirror.*` Postgres projection
(fast reads, DuckLake parity) over a **vendored** Iceberg SQL catalog — loom
owns `update_table`, which is what makes atomic pointer+mirror commits possible.

Original decisions: adopt iceberg-rust · coexist (maybe replace later) ·
pointer+mirror storage · read-path first · vendor the catalog.

## Tracking moved to the registers

Item-level tracking for this adapter (shipped slices, deferred work, known
defects) now lives in the documentation registers, filterable by `area:iceberg`:

- shipped slices → [`docs/ROADMAP.md`](../ROADMAP.md) — `bash tools/docs.sh query done --area iceberg`
- deferred capabilities → [`docs/FUTURE.md`](../FUTURE.md) — `bash tools/docs.sh query open --area iceberg`
- known defects/gaps → [`docs/ISSUES.md`](../ISSUES.md)

This file is kept as the spike's narrative ("What it is" above); see CLAUDE.md
("Documentation registers") for the grammar and tooling.
