# Iceberg Adapter — Roadmap

> Status snapshot as of 2026-06-17. Captures what the Iceberg table-format
> adapter is, what's shipped, and what's deferred. This is a spike/roadmap
> note, not a spec — each remaining slice gets its own design doc before
> implementation.

## What it is

An Iceberg table-format adapter that **coexists** with DuckLake. It serves
loom's `core::Catalog` from a loom-owned `iceberg_mirror.*` Postgres projection
(fast reads, DuckLake parity) over a **vendored** Iceberg SQL catalog — loom
owns `update_table`, which is what makes atomic pointer+mirror commits possible.

Original decisions: adopt iceberg-rust · coexist (maybe replace later) ·
pointer+mirror storage · read-path first · vendor the catalog.

## Tracking moved to GitHub Issues

Item-level tracking for this adapter (roadmap slices, deferred work, known
defects) now lives in the GitHub issue tracker, filterable by `area:iceberg`:

- committed slices → `gh issue list --label roadmap --label "area:iceberg"`
- deferred capabilities → `gh issue list --label idea --label "area:iceberg"`
- known defects/gaps → `gh issue list --label bug --label "area:iceberg"`

This file is kept as the spike's narrative ("What it is" above); see CLAUDE.md
("Work tracking (GitHub Issues)") for the labels and tooling.
