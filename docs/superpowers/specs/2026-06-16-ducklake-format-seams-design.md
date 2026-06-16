# Design: DuckLake behind clean format seams

> **Type:** design spec. **Date:** 2026-06-16.
> **Supersedes the action items of:** `docs/spikes/2026-06-12-ducklake-trait-boundary.md`.
> **Decision:** override the spike's "do not abstract further now" — reshape `core`
> and the SQL-generation layer so a future Iceberg adapter slots in, **validated
> against Iceberg's real model on paper, building zero Iceberg code.**

## Why now (and the reservation we are overriding)

The spike verified that DuckLake already sits behind three ports (catalog read,
snapshot-commit write, serving) and recommended *not* abstracting further until a
real second format forces the boundary. Its core technical claim still holds: **without
a forcing function you draw the boundary in the wrong place, and it goes unused.**

We override the *recommendation* but **honour the claim** by supplying the forcing
function cheaply: Iceberg is a **design constraint**, not a build target. The spec's
load-bearing artifact is a written DuckLake×Iceberg mapping table (§WS1) that shapes
every neutral type against Iceberg's actual metadata model — so the boundary is drawn
against two concrete formats, not one-plus-a-guess, **without** paying to build or test
an unused adapter. The two loom-specific Iceberg-parity features the maintainer named
(an Iceberg catalog in Postgres for fast reads; inline writes) are **future adapter
work**, explicitly out of scope here (see §Non-goals).

## Two orthogonal swap axes

The spike conflated two independent axes. Separating them is what makes this tractable:

1. **Table-format axis** (DuckLake → Iceberg): changes *file metadata*, *column types*,
   and the *catalog conceptual model*. This is leaks **#1, #2, #4**. → Workstream 1.
2. **Serving-engine axis** (DuckDB → another engine): changes *SQL dialect*. This is leak
   **#3**. → Workstream 2.

Iceberg-as-a-table-format does **not** touch the serving engine — loom still reads
Parquet through DataFusion/DuckDB regardless of catalog format. WS2 is therefore a
distinct, more speculative axis; it is included by explicit maintainer decision, scoped
as a seam that changes nothing observable today (DuckDB stays the only dialect).

## Current state (verified 2026-06-16)

- Write metadata types `core::snapshot::{ColumnSpec, ColumnStat, DataFile}` carry
  DuckLake vocabulary: `footer_size` (Parquet footer), `value_count`
  (`= record_count − null_count`, DuckLake's *non-null* count), VARCHAR `min/max`,
  and `ColumnSpec.ty` as a DuckLake type string.
- `datafusion-io/src/write.rs` already computes **typed** min/max via a private `Bound`
  enum (`write.rs:171`) then *stringifies at the boundary* (`to_stat_string`, `:283`)
  because `ColumnStat.min/max` are `Option<String>`; it sets `value_count` (`:286`) and
  reads `footer_size` from the Parquet trailer (`:302`).
- `postgres/src/snapshot.rs::write_data_file` is the **only** place that speaks catalog
  dialect (footer_size → `ducklake_data_file`; value_count → `ducklake_file_column_stats`;
  VARCHAR min/max; `file_format 'parquet'` hardcoded at `:414`). The DuckLake dialect is
  already almost entirely confined to this adapter — `core`'s types merely *carry its
  vocabulary*.
- `core::logical_type::physical_affinity` maps loom logical `BaseType` → DuckLake physical
  strings (`int32`/`int64`/`varchar`/…); used by `satisfies()` for dataset→model binding.
- SQL is generated DuckDB-flavored in `query-api/src/sql.rs` (`compile_select`/`compile_chain`)
  and flows *through* `ServingEngine::fetch_rows(sql, params)`, not behind it. Both engines
  today (`EmbeddedDuckDb`, `QuackServingEngine`) speak DuckDB dialect.
- `query-api/src/serving.rs:52` already defines `ActionEngine` ("the seam a future Iceberg
  backend swaps in") with `EmbeddedDuckDbWriter` doing inline inserts. **The inline-write
  seam already exists** — Iceberg parity there is a future adapter impl, not new design.

---

## Workstream 1 — Table-format metadata boundary (leaks #1, #2, #4)

`core` stops speaking DuckLake's physical dialect and speaks a logical/typed contract;
all DuckLake encoding/decoding lives in the postgres adapter (which already owns most of
it). Validated against Iceberg by the mapping table below.

### 1.1 Typed stat bounds (leak #1)

Lift `datafusion-io`'s private `Bound` enum into `core::snapshot` as:

```rust
/// A typed scalar stat bound. Each table-format adapter encodes it its own way
/// (DuckLake → VARCHAR string; Iceberg → typed binary lower/upper bound).
#[derive(Clone, Debug, PartialEq)]
pub enum StatValue {
    Bool(bool),
    I32(i32),
    I64(i64),
    F32(f32),
    F64(f64),
    Str(String),
}
```

These six variants are exactly what Parquet column statistics produce today
(`write.rs:205-231`). Date/Timestamp columns surface as `I32`/`I64` physical stats, so no
temporal variant is needed yet; the enum is extensible when a format needs more.

`ColumnStat` becomes:

```rust
#[derive(Clone, Debug, PartialEq)]
pub struct ColumnStat {
    pub column: String,            // column NAME; each adapter resolves to its id space
    pub null_count: i64,
    pub column_size_bytes: i64,
    pub min: Option<StatValue>,
    pub max: Option<StatValue>,
    // value_count REMOVED — derived per-adapter (see mapping table)
}
```

The VARCHAR encoding (`to_stat_string`) moves out of `datafusion-io` into the postgres
adapter as `to_ducklake_stat_string(&StatValue) -> String`.

### 1.2 File metadata neutralization (leak #1)

```rust
/// The on-storage file format of a registered data file.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FileFormat {
    Parquet,
    // Orc / Avro added when a format/engine needs them.
}

#[derive(Clone, Debug, PartialEq)]
pub struct DataFile {
    pub path: String,
    pub path_is_relative: bool,
    pub file_format: FileFormat,        // was hardcoded 'parquet' in the adapter
    pub record_count: i64,
    pub file_size_bytes: i64,
    pub column_stats: Vec<ColumnStat>,
    pub parquet_footer_size: Option<i64>, // physical-Parquet detail; formats may ignore
}
```

`footer_size: i64` → `parquet_footer_size: Option<i64>`: it is a Parquet-physical fact
DuckLake records and Iceberg ignores. Producers that know it (`datafusion-io`) set
`Some`; the DuckLake adapter requires it (errors on `None` for a Parquet file, since
`ducklake_data_file.footer_size` is non-null).

### 1.3 Logical column types — full neutralization (leak #2)

`core` speaks **only** loom logical types; the DuckLake adapter owns the bidirectional
`ducklake ↔ logical` mapping on **both** read and write.

- `core::snapshot::ColumnSpec.ty` and `core::catalog::ColumnDef.ty` carry a loom **logical
  type string** (canonical: `integer`/`long`/`double`/`boolean`/`string`/`date`/`timestamp`,
  per `resolve_logical`), not a DuckLake string.
- `core::logical_type::physical_affinity` (the logical→DuckLake-string table) **moves out
  of `core`** into the postgres adapter as a bidirectional mapping:
  - write: `ducklake_physical_type(logical: BaseType) -> &'static str` (e.g. `Long → "int64"`).
  - read: `logical_from_ducklake(physical: &str) -> Option<BaseType>` (e.g. `"int32" → Integer`,
    `"int64" → Long`), preserving the no-implicit-widening rule.
- `core::logical_type::satisfies` becomes logical-vs-logical: the read-side
  `ColumnDef.ty` is already logical (the adapter mapped it), so binding compares a
  declared logical property type against a logical column type. The widening discipline
  (Integer = 32-bit only) now lives in the adapter's `logical_from_ducklake` direction.
- `datafusion-io/src/infer.rs` maps Arrow → loom-logical (instead of Arrow → DuckLake
  string).

If the catalog holds a physical type loom has no logical name for, the read-side mapping
returns `None`; `Catalog::schema()` surfaces it as an explicit error rather than leaking a
raw DuckLake string back into `core` (keeps the boundary honest; the closed vocabulary
stays authoritative).

### 1.4 Conceptual deleak (leak #4)

- `core::catalog` module doc reworded format-neutral: "a read-only view over the active
  table-format catalog; the table-format adapter (DuckLake today) populates it."
- `SnapshotId` doc drops "DuckLake catalog-global"; the type `SnapshotId(pub i64)` is
  **unchanged** — i64 snapshot/version ids are portable (Iceberg and Delta key by i64 too).
- `core::snapshot` module doc reworded to describe the register-only commit primitive
  in format-neutral terms.

### 1.5 The Iceberg mapping table — the forcing-function artifact

Every neutral `core` field, mapped to both formats. The DuckLake column is implemented;
the Iceberg column is **paper validation only** — it shapes the boundary and is the
acceptance check that we did not draw it DuckLake-blind.

| Neutral `core` field | DuckLake adapter (built) | Iceberg adapter (paper) |
|---|---|---|
| `DataFile.path` / `path_is_relative` | `ducklake_data_file.path` / `path_is_relative` | `data_file.file_path` (URI) |
| `DataFile.file_format` | was hardcoded `'parquet'` → now from field | `data_file.file_format` |
| `DataFile.record_count` | `record_count` | `data_file.record_count` |
| `DataFile.file_size_bytes` | `file_size_bytes` | `file_size_in_bytes` |
| `DataFile.parquet_footer_size: Option` | `footer_size` (required for Parquet) | *(ignored — not tracked)* |
| `ColumnStat.column` (name) | resolve → `column_id` | resolve → `field_id` |
| `ColumnStat.null_count` | `null_count`; `contains_null = null_count > 0` | `null_value_counts[field_id]` |
| *(derived, not stored)* value_count | `record_count − null_count` (**non-null**) | `value_counts[field_id]` = `record_count` (**incl. nulls**) |
| `ColumnStat.column_size_bytes` | `column_size_bytes` | `column_sizes[field_id]` |
| `ColumnStat.min` / `max: StatValue` | VARCHAR `min_value` / `max_value` | typed-binary `lower_bounds` / `upper_bounds[field_id]` |
| `ColumnSpec.ty` / `ColumnDef.ty` (logical) | ↔ ducklake type string | ↔ Iceberg primitive type |
| `SnapshotId(i64)` | `ducklake_snapshot.snapshot_id` | snapshot id / sequence number (i64) |

**What the table proves (the acceptance criteria for "shaped against Iceberg"):**
- **value_count must be derived, not stored:** Iceberg `value_counts` *include* nulls,
  DuckLake's *exclude* them. Storing `null_count` + `record_count` lets each adapter derive
  its own convention; a stored `value_count` would bake in DuckLake's.
- **bounds must be typed, not VARCHAR:** DuckLake stringifies; Iceberg writes typed binary.
  A typed `StatValue` is the only neutral form that serves both.
- **stats keyed by name, resolved per-adapter:** DuckLake → `column_id`, Iceberg →
  `field_id`. A name is portable; an id is not.
- **file_format must be explicit:** Iceberg records it per file; a hardcoded `'parquet'`
  in `core` would block ORC/Avro.

### 1.6 Consumer ripple (mechanical)

- `datafusion-io/src/write.rs`: `Bound` → `core::StatValue`; stop calling `to_stat_string`
  and stop computing `value_count`; set `parquet_footer_size: Some(...)`. `WrittenFile`
  carries `core::ColumnStat` with typed bounds.
- `datafusion-io/src/infer.rs`: Arrow → loom-logical type strings.
- `ingest/src/materialize.rs`: build the new `DataFile`/`ColumnSpec` shapes.
- `transform/src/conform.rs`: uses `ColumnSpec`; adjust to logical `ty`.
- `postgres/src/{snapshot,catalog}.rs`: own all ducklake encode (`to_ducklake_stat_string`,
  `ducklake_physical_type`, derive value_count, require footer_size) and decode
  (`logical_from_ducklake` in `schema()`).

---

## Workstream 2 — SQL-dialect seam (leak #3)

The serving-engine axis. Included by maintainer decision; scoped so **nothing observable
changes today** (DuckDB stays the only dialect). The win: SQL generation stops being
hardwired to DuckDB.

### 2.1 The `SqlDialect` trait

Capture the finite dialect knobs currently hardcoded in `sql.rs`:

```rust
pub trait SqlDialect: Send + Sync {
    fn quote_ident(&self, id: &str) -> String;     // "ident" today
    fn placeholder(&self, one_based: usize) -> String; // "?" today ($n for pg-style)
    fn limit_clause(&self, n: u32) -> String;       // "LIMIT n" today
    fn bool_literal(&self, b: bool) -> &'static str; // TRUE/FALSE
    fn count_star(&self) -> &'static str;           // COUNT(*)
    fn sum_coalesced(&self, col: &str) -> String;   // COALESCE(SUM(col), 0)
    // avg/min/max spellings as needed
}
```

`compile_select`/`compile_chain` take `&dyn SqlDialect` and consult it instead of
inlining literals. The injection boundary is unchanged — identifiers still come only from
trusted ontology/ACL metadata and are quoted via the dialect; values stay bound `?`
params (or the dialect's placeholder).

### 2.2 The only impl: `DuckDbDialect`

Reproduces today's output **byte-for-byte**. `ServingEngine` gains
`fn dialect(&self) -> &dyn SqlDialect` (default-providing `DuckDbDialect`) so engine and
dialect cannot desync; the handler compiles with `serving.dialect()`.

A future non-DuckDB engine ships a new `SqlDialect` impl, not a `sql.rs` rewrite.

---

## Testing strategy

- **DuckDB read-back interop oracle** (`datafusion-io/tests/ducklake_interop.rs`) staying
  green is the primary proof the WS1 refactor preserved DuckLake byte-fidelity end to end.
- **postgres fixture tests** (`snapshot`/`catalog`): prove encoding/decoding moved
  correctly (StatValue→VARCHAR, value_count derivation, logical↔ducklake type round-trip,
  footer_size required).
- **query-api golden-SQL tests**: assert `compile_select`/`compile_chain` through
  `DuckDbDialect` produce byte-identical SQL to today (WS2 is a no-op).
- **New pure-logic unit tests** (sibling `tests/*.rs`, per the no-inline-tests rule):
  `to_ducklake_stat_string`, `ducklake_physical_type`/`logical_from_ducklake` round-trip,
  value_count derivation, `logical_from_ducklake` rejects unknown physical types.

## Non-goals (YAGNI guards)

- **No Iceberg adapter code.** The Iceberg column of the mapping table is paper-only.
- **No second SQL dialect.** WS2 ships `DuckDbDialect` only.
- **No Iceberg catalog-in-Postgres** and **no inline-write changes** — both are future
  Iceberg-adapter parity work; the inline seam (`ActionEngine`/`EmbeddedDuckDbWriter`)
  already exists.
- **No change to `SnapshotId`** or to the three existing port traits' method shapes
  (only `ColumnDef.ty`/`ColumnSpec.ty` payload type and the metadata structs change).

## Plan structure

Two independently-shippable plans, each green on its own:

1. **WS1 — table-format metadata boundary** (#1, #2, #4). The maintainer's actual
   Iceberg-parity driver. Do first.
2. **WS2 — SQL-dialect seam** (#3). The more speculative seam. Do second.
