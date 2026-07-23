# Spike: putting "everything DuckLake" behind a trait

> **Type:** exploration spike (read-only; no code changes). **Date:** 2026-06-12.
> **Question:** what would it take to abstract DuckLake behind a trait so loom could
> swap in another table format (Iceberg, Delta, …) later?
> **Verdict:** the trait *structure* already exists — DuckLake sits behind three ports
> today. Do **not** abstract further now; keep the existing ports clean and let a real
> second format drive the abstraction. Rationale below. See also the standing principle
> *opinionated, not pluggable*.

## TL;DR

loom is already ports-and-adapters, and DuckLake already lives behind three trait seams.
"Put it behind a trait" is ~80% done. The work a *real* second format would force is not
"add a trait" — it is fixing four localized **leaks** where DuckLake-specific vocabulary
has bled up into `core` (the format-neutral port layer). Three of the four are cheap; one
(the write-side metadata types) is the one knowingly-leaky seam. None is worth fixing
speculatively: building+testing a second adapter with no consumer is exactly the
speculative generality the project avoids, and without a real second format you would
almost certainly draw the boundary wrong.

## The three seams that already exist

| Seam | Trait (port, in `core`/`query-api`) | DuckLake adapter | LOC |
|---|---|---|---|
| **Catalog read** | `core::Catalog` — `current_snapshot`/`snapshots`/`files`/`schema` | `postgres/src/catalog.rs` reads `ducklake_*` | 138 |
| **Snapshot-commit write** | `core::ControlPlane` + `core::Tx` — `create_table`/`append_files` → snapshot | `postgres/src/snapshot.rs` natively writes `ducklake_*`; `memory` fake also impls it | 448 |
| **Serving / query** | `query-api::ServingEngine` — `fetch_rows(sql, params)` | `EmbeddedDuckDb` + `QuackServingEngine` | 283 |

Each already has at least two implementations (real + fake, or embedded + remote), so the
ports are real, not nominal. Swapping the table format means writing new adapters behind
these same traits — the seam is there.

## The four leaks (what a swap would actually have to fix)

### 1. Write-side metadata types are DuckLake-shaped — *the leaky seam*

`core::snapshot::{DataFile, ColumnStat}` mirror the `ducklake_data_file` / `ducklake_column`
catalog columns almost 1:1:

- `DataFile`: `path_is_relative`, `record_count`, `file_size_bytes`, **`footer_size`** (a
  Parquet-footer concept), `column_stats`.
- `ColumnStat`: `min`/`max` as `Option<String>` (DuckLake's VARCHAR stat encoding),
  `null_count`, **`value_count`** (commented in-source: "DuckLake `value_count =
  num_values - null_count`"), `column_size_bytes`.

These types live in the supposedly format-neutral `core`, but they speak DuckLake's
physical-catalog dialect. An Iceberg adapter wants manifest-entry metadata, not a Parquet
footer size and VARCHAR-encoded bounds. This is the **one seam that is knowingly leaky** and
the main thing a second format would have to redesign (an abstract file-metadata type that
each format maps to/from).

### 2. `ColumnSpec.ty` and type affinities are DuckLake type strings — *half-solved*

`core::snapshot::ColumnSpec.ty` is a DuckLake type string (`"int64"`, `"varchar"`), and
`core::logical_type` physical affinities are DuckLake strings (`int32`/`int64`/`float64`/
`varchar`/…). **Already half-abstracted:** the `BaseType` logical vocabulary exists and maps
logical → DuckLake. A second format needs only a second affinity table (logical → Iceberg
type). Cheap; the vocabulary was built for exactly this kind of evolution.

### 3. SQL dialect flows *through* `ServingEngine`, not behind it

`ServingEngine::fetch_rows(sql, params)` is dialect-agnostic at the signature, but the SQL it
receives is generated DuckDB-flavored by `query-api/src/handler.rs` + `render.rs`. A
non-DuckDB serving engine would need the SQL generation to change dialect — a coupling that
sits *above* the trait. Making serving truly pluggable means a SQL-dialect seam (or an
engine-native query builder), not just a new `ServingEngine` impl.

### 4. Conceptual-model leak (not type-level)

`core::Catalog`'s module doc frames it as "a read-only view over DuckLake's catalog; DuckLake
(the DuckDB client) writes it." That is the DuckLake single-catalog assumption; the native
writer (`Tx::create_table`/`append_files`) already partly reverses it (loom writes the catalog
rows too). `SnapshotId(pub i64)` is **not** a leak — Iceberg and Delta also key snapshots/
versions by i64, so the integer id is portable enough.

## Cost / benefit

- **Keeping the seams: ~free.** The three ports already exist and are exercised by multiple
  impls. No action needed to "keep the option open."
- **Making it genuinely format-pluggable: medium effort, speculative.** It needs (1) an
  abstract file-metadata type each format maps to/from, (2) per-format type affinities (the
  vocabulary is ready), and (3) a SQL-dialect seam. That is a real chunk of design — and
  you would be building and testing a second adapter that has **no consumer**, drawing the
  abstraction boundary blind. The second real format is the forcing function that reveals the
  correct boundary; without it, speculative abstraction is likely to be wrong *and* unused.

## Recommendation

**Do not abstract further now.** Instead:

1. **Treat `core::snapshot::{DataFile, ColumnStat}` as the one knowingly-leaky seam** and
   resist letting *more* DuckLake-isms into `core`. Keeping new coupling out of the port layer
   is the cheap, high-value discipline.
2. **Let a real second-format requirement drive the abstraction.** When (if) Iceberg/Delta
   becomes a goal, that adapter is the forcing function: redesign the write-side metadata type
   and add a SQL-dialect seam *then*, informed by two concrete formats instead of one plus a
   guess.
3. The pay-off of today's structure is that the option stays open for near-zero cost — which
   is the right place to be for a flexibility that is not (yet) a goal.

## Pointers (for whoever picks this up later)

- Write port: `core/src/snapshot.rs`, `core/src/transaction.rs`; DuckLake impl
  `postgres/src/snapshot.rs`, `postgres/src/transaction.rs`.
- Read port: `core/src/catalog.rs`; DuckLake impl `postgres/src/catalog.rs`.
- Serving port: `query-api/src/serving.rs`; SQL generation `query-api/src/handler.rs` +
  `render.rs`.
- Type vocabulary: `core/src/logical_type.rs` (the logical → physical affinity table).
- The single-catalog write recipe this all rests on (design doc in git history:
  `2026-06-09-ducklake-single-catalog-write-recipe`).
