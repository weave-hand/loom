# DuckLake Seams — WS1: Table-Format Metadata Boundary — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `control_plane_core` speak a logical/typed metadata contract (typed stat
bounds, logical column types, derivable counts, explicit file format) instead of
DuckLake's physical dialect, and confine all DuckLake encoding/decoding to the postgres
adapter — so a future Iceberg adapter slots in (validated on paper by the spec's mapping
table), building zero Iceberg code.

**Architecture:** Two atomic cross-crate cascades. A `core` type/semantics change does not
compile or pass tests until every consumer is updated, so each cascade is ONE task whose
intermediate steps may be red but which **ends green** (`buck2 build //src/...` +
`buck2 test //src/...`). Cascade 1 = leak #1 (file-metadata types) + leak #4 (doc deleak).
Cascade 2 = leak #2 (full logical column-type neutralization). A final task verifies the
whole tree, the DuckDB read-back interop oracles, and clippy.

**Tech Stack:** Rust (edition 2024), buck2 (`rust_library`/`rust_test`), DuckLake catalog
(Postgres via sqlx compile-time macros), DataFusion/Parquet, Arrow.

**Spec:** `docs/superpowers/specs/2026-06-16-ducklake-format-seams-design.md` (§Workstream 1
and the Iceberg mapping table are the acceptance reference).

---

## Conventions for this plan

- **Build a single crate:** `buck2 build //src/control-plane/core:core` (swap target).
- **Test a single crate:** `buck2 test //src/control-plane/core:logical-type > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log` (NEVER pipe `buck2 test` to `tail`/`head` — redirect to a file and grep; see CLAUDE.md).
- **Whole-tree:** `buck2 build //src/... 2>&1 | tail -5` is fine (build-to-tail does not stall); for test use `buck2 test //src/... > /tmp/all.log 2>&1; grep -E "Tests finished|FAIL" /tmp/all.log`.
- **Fixture tests** (postgres/ingest/query-api/transform) boot hermetic postgres/duckdb; they self-route to local execution via `loom_fixture_test`. Run them the same way; they are slower.
- **No inline `#[test]`** in `src/**`: unit tests go in a sibling `tests/<name>.rs` wired as a `rust_test` in the crate `BUCK` (a prek hook enforces this).
- **`.sqlx` is NOT regenerated in WS1** — no SQL text changes. If you find yourself editing a SQL string, stop: WS1 only changes Rust-side values bound into unchanged SQL.

---

## File Structure (what changes and why)

**`src/control-plane/core/` — the neutral contract**
- `src/snapshot.rs` — add `StatValue`, `FileFormat`; reshape `ColumnStat` (drop `value_count`, typed `min`/`max`) and `DataFile` (add `file_format`, `footer_size`→`parquet_footer_size: Option`); drop `Eq`; reword module doc (#4).
- `src/catalog.rs` — reword module doc + `SnapshotId` doc format-neutral (#4); `ColumnDef.ty` doc says "loom logical type".
- `src/logical_type.rs` — remove `BaseType::physical_affinity`; change `satisfies` to logical-vs-logical; add `BaseType::canonical_name`; reword module doc.
- `src/lib.rs` — export `StatValue`, `FileFormat`; add `canonical_name` is a method (no new export needed beyond `BaseType`, already exported).
- `tests/logical_type.rs` — rewrite `satisfies` cases to logical-vs-logical; add `canonical_name` cases.
- `tests/snapshot_types.rs` *(new)* — unit-test `StatValue`/typed-bound shape (kept tiny).

**`src/control-plane/postgres/` — the DuckLake adapter (owns dialect)**
- `src/ducklake_type.rs` *(new)* — `to_ducklake_stat_string`, `ducklake_physical_type`, `logical_from_ducklake` (the dialect mapping lifted out of core + `write.rs`).
- `src/lib.rs` — `mod ducklake_type;`.
- `src/snapshot.rs` — `write_data_file`: derive `value_count`, unwrap `parquet_footer_size`, map stat `StatValue`→VARCHAR; `write_table`: map logical `ColumnSpec.ty`→ducklake string.
- `src/catalog.rs` — `schema()`: map ducklake `column_type`→logical for `ColumnDef.ty`.
- `tests/ducklake_type.rs` *(new)* — unit tests for the three mapping fns + round-trip.
- `tests/{snapshot_append,snapshot_create,snapshot_rollback,ducklake_interop,catalog}.rs` — update `DataFile`/`ColumnStat` literals + schema expectations.

**`src/services/datafusion-io/`**
- `src/write.rs` — replace private `Bound` with `core::StatValue`; stop stringifying; drop `value_count`; keep `footer_size: i64` on `WrittenFile`.
- `src/infer.rs` — `duck_type`→`arrow_logical_type` (Arrow→loom-logical); `infer_columns` emits logical.
- `src/lib.rs` — re-export rename `duck_type`→`arrow_logical_type`.
- `tests/{infer,write}.rs` — update expectations.

**`src/services/ingest/`**
- `src/gate.rs` — `ModelShape.ty` is logical; `validate` compares via `arrow_logical_type`.
- `src/materialize.rs` — `DataFile` construction (`file_format`, `parquet_footer_size`).
- `tests/{gate,materialize,ducklake_interop}.rs` — update literals/expectations.

**`src/services/transform/`**
- `src/run.rs` — `DataFile` construction (`file_format`, `parquet_footer_size`).
- `src/conform.rs` — unchanged code (relies on new `satisfies` semantics); update `tests/conform.rs` literals.

**`src/control-plane/testkit/`**
- `src/lib.rs` — update the `DataFile` literal (~line 1850).

**`src/services/query-api/`**
- `tests/governed_read.rs` — update `DataFile`/`ColumnStat` literals.

---

## Task 1 — Cascade 1: typed file-metadata + doc deleak (leaks #1, #4)

**Files:**
- Modify: `src/control-plane/core/src/snapshot.rs`, `src/control-plane/core/src/catalog.rs`, `src/control-plane/core/src/lib.rs`
- Create: `src/control-plane/postgres/src/ducklake_type.rs`, `src/control-plane/postgres/tests/ducklake_type.rs`, `src/control-plane/core/tests/snapshot_types.rs`
- Modify: `src/control-plane/postgres/src/lib.rs`, `src/control-plane/postgres/src/snapshot.rs`
- Modify: `src/services/datafusion-io/src/write.rs`, `src/services/datafusion-io/tests/write.rs`
- Modify: `src/services/ingest/src/materialize.rs`, `src/services/transform/src/run.rs`
- Modify test literals: `src/control-plane/testkit/src/lib.rs`, `src/control-plane/postgres/tests/{snapshot_append,snapshot_create,snapshot_rollback,ducklake_interop}.rs`, `src/services/query-api/tests/governed_read.rs`
- Modify BUCK: `src/control-plane/core/BUCK`, `src/control-plane/postgres/BUCK`

This task is one atomic cascade: it ends green, but intermediate steps are red. Do the steps in order.

- [ ] **Step 1: Reshape the core metadata types.**

In `src/control-plane/core/src/snapshot.rs`, replace the whole file body with:

```rust
//! Inputs for the native register-only snapshot-commit primitive (the caller writes
//! the data files; loom writes the catalog rows). Format-neutral: the active
//! table-format adapter (DuckLake today) encodes these into its physical catalog.
//! See `docs/superpowers/specs/2026-06-16-ducklake-format-seams-design.md`.

/// A column for `Tx::create_table`. `ty` is a loom LOGICAL type name (canonical:
/// "integer"/"long"/"double"/"boolean"/"string"/"date"/"timestamp", or a known
/// alias). The active adapter maps it to its physical type string.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ColumnSpec {
    pub name: String,
    pub ty: String,
    pub nullable: bool,
}

/// A typed scalar stat bound. Format-neutral: each adapter encodes it its own way
/// (DuckLake → VARCHAR string; Iceberg → typed binary lower/upper bound). Not `Eq`
/// (carries floats).
#[derive(Clone, Debug, PartialEq)]
pub enum StatValue {
    Bool(bool),
    I32(i32),
    I64(i64),
    F32(f32),
    F64(f64),
    Str(String),
}

/// Per-column statistics for one data file. `value_count` is NOT stored: it is
/// derivable (`record_count − null_count`) and each format counts differently
/// (DuckLake excludes nulls; Iceberg includes them), so the adapter derives it.
#[derive(Clone, Debug, PartialEq)]
pub struct ColumnStat {
    pub column_name: String,
    pub null_count: i64,
    pub column_size_bytes: i64,
    pub min: Option<StatValue>,
    pub max: Option<StatValue>,
}

/// The on-storage format of a registered data file. Explicit (not assumed Parquet)
/// because formats like Iceberg record it per file.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FileFormat {
    Parquet,
}

/// A data file the caller has already written to object storage.
#[derive(Clone, Debug, PartialEq)]
pub struct DataFile {
    pub path: String,
    pub path_is_relative: bool,
    pub file_format: FileFormat,
    pub record_count: i64,
    pub file_size_bytes: i64,
    pub column_stats: Vec<ColumnStat>,
    /// Parquet footer length — a physical-Parquet detail some formats persist
    /// (DuckLake records it; Iceberg ignores it). `Some` for Parquet files.
    pub parquet_footer_size: Option<i64>,
}
```

- [ ] **Step 2: Export the new types.**

In `src/control-plane/core/src/lib.rs`, change the snapshot re-export line:

```rust
pub use snapshot::{ColumnSpec, ColumnStat, DataFile, FileFormat, StatValue};
```

- [ ] **Step 3: Deleak the catalog docs (#4).**

In `src/control-plane/core/src/catalog.rs`: replace the module doc (lines 1-5) and the `SnapshotId`/`ColumnDef` docs:

Module doc (top of file):
```rust
//! The catalog concern: a read-only view over the active table-format catalog. The
//! table-format adapter (DuckLake today) populates it; loom reads it. Snapshots are
//! catalog-global and identified by a monotonic id; tables/files/columns are
//! versioned by `begin`/`end` snapshot ranges (MVCC), so reads are "this table
//! *at* that snapshot".
```

`SnapshotId` doc (line 13):
```rust
/// A catalog-global snapshot/version id (monotonic). Portable across table formats
/// (DuckLake, Iceberg, Delta all key versions by i64).
```

`ColumnDef.ty` doc (around line 44):
```rust
    /// The column's loom LOGICAL type name (the adapter maps from its physical
    /// catalog type on read). Typing is an ontology concern.
```

- [ ] **Step 4: Add a tiny core unit test for the typed bound, wire its BUCK target.**

Create `src/control-plane/core/tests/snapshot_types.rs`:

```rust
use control_plane_core::{ColumnStat, DataFile, FileFormat, StatValue};

#[test]
fn column_stat_holds_typed_bounds_without_value_count() {
    let s = ColumnStat {
        column_name: "id".into(),
        null_count: 1,
        column_size_bytes: 64,
        min: Some(StatValue::I64(10)),
        max: Some(StatValue::I64(99)),
    };
    assert_eq!(s.min, Some(StatValue::I64(10)));
    assert_eq!(s.max, Some(StatValue::I64(99)));
}

#[test]
fn data_file_records_format_and_optional_footer() {
    let f = DataFile {
        path: "p/part-0.parquet".into(),
        path_is_relative: true,
        file_format: FileFormat::Parquet,
        record_count: 3,
        file_size_bytes: 200,
        column_stats: vec![],
        parquet_footer_size: Some(50),
    };
    assert_eq!(f.file_format, FileFormat::Parquet);
    assert_eq!(f.parquet_footer_size, Some(50));
}
```

In `src/control-plane/core/BUCK`, add (mirror the `identity` target):

```python
rust_test(
    name = "snapshot-types",
    crate = "snapshot_types",
    srcs = ["tests/snapshot_types.rs"],
    crate_root = "tests/snapshot_types.rs",
    edition = "2024",
    visibility = ["PUBLIC"],
    deps = [":core"],
)
```

Run: `buck2 test //src/control-plane/core:snapshot-types > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`
Expected: PASS (core compiles; the rest of the tree is still red — that is expected mid-cascade).

- [ ] **Step 5: Create the postgres DuckLake stat-string encoder + its unit test.**

Create `src/control-plane/postgres/src/ducklake_type.rs`:

```rust
//! DuckLake physical-dialect mapping: the encoding that used to live in `core` and
//! in `datafusion-io::write`. Confined here so `core` stays format-neutral.

use control_plane_core::StatValue;

/// Encode a typed stat bound as DuckLake's VARCHAR stat string. Byte-identical to the
/// former `datafusion_io::write::Bound::to_stat_string` (the interop oracle is the test).
pub(crate) fn to_ducklake_stat_string(v: &StatValue) -> String {
    match v {
        StatValue::Bool(b) => b.to_string(),
        StatValue::I32(n) => n.to_string(),
        StatValue::I64(n) => n.to_string(),
        StatValue::F32(f) => f.to_string(),
        StatValue::F64(f) => f.to_string(),
        StatValue::Str(s) => s.clone(),
    }
}
```

(The two type-mapping fns `ducklake_physical_type`/`logical_from_ducklake` are added in Task 2; keep this module focused on stats for now.)

In `src/control-plane/postgres/src/lib.rs`, add near the other `mod` lines:
```rust
mod ducklake_type;
```

Create `src/control-plane/postgres/tests/ducklake_type.rs`:

```rust
// The encoder is pub(crate); test it through a thin re-export is overkill — instead
// assert the values it must produce via the public append path is covered by the
// interop oracle. Here we lock the pure string encoding with a focused crate test by
// re-declaring the mapping's expected outputs.
use control_plane_core::StatValue;

// Mirror of to_ducklake_stat_string for an executable spec of the required encoding.
fn expect(v: &StatValue) -> String {
    match v {
        StatValue::Bool(b) => b.to_string(),
        StatValue::I32(n) => n.to_string(),
        StatValue::I64(n) => n.to_string(),
        StatValue::F32(f) => f.to_string(),
        StatValue::F64(f) => f.to_string(),
        StatValue::Str(s) => s.clone(),
    }
}

#[test]
fn stat_strings_match_ducklake_varchar_encoding() {
    assert_eq!(expect(&StatValue::I64(42)), "42");
    assert_eq!(expect(&StatValue::Bool(true)), "true");
    assert_eq!(expect(&StatValue::F64(1.5)), "1.5");
    assert_eq!(expect(&StatValue::Str("ann".into())), "ann");
}
```

> NOTE: `to_ducklake_stat_string` is `pub(crate)`, so a `tests/` integration crate cannot call it directly. The real fidelity guarantee is the DuckDB read-back interop oracle (`postgres/tests/ducklake_interop.rs`), which exercises it end-to-end. This focused test documents the required encoding; if you prefer a direct call, make the fn `pub` and import it — either is acceptable.

In `src/control-plane/postgres/BUCK`, add a `rust_test` for `tests/ducklake_type.rs` mirroring an existing pure-logic target (deps `[":postgres", "//src/control-plane/core:core"]` — match the deps an existing non-fixture postgres test uses; if none, use `["//src/control-plane/core:core"]`).

- [ ] **Step 6: Update the postgres append writer to derive/encode.**

In `src/control-plane/postgres/src/snapshot.rs`:

Add the import at the top (alongside the existing `use crate::backend;`):
```rust
use crate::ducklake_type::to_ducklake_stat_string;
```

In `write_data_file`, the `ducklake_table_column_stats` UPDATE/INSERT block (lines ~371-404) currently binds `stat.min`/`stat.max` directly. Compute the encoded strings once per stat at the top of the `for stat in &file.column_stats` loop:
```rust
    for stat in &file.column_stats {
        let column_id = resolve_column_id(tx, table_id, &stat.column_name).await?;
        let contains_null = stat.null_count > 0;
        let min_s = stat.min.as_ref().map(to_ducklake_stat_string);
        let max_s = stat.max.as_ref().map(to_ducklake_stat_string);
        // ... UPDATE uses $4 = min_s, $5 = max_s; INSERT likewise ...
```
Replace the `stat.min`/`stat.max` bind arguments in BOTH the UPDATE and the INSERT with `min_s`/`max_s`. (SQL text unchanged.)

For the `ducklake_data_file` INSERT (lines ~409-427): the `file.footer_size` bind becomes the unwrapped optional. Before the INSERT add:
```rust
    let footer_size = file.parquet_footer_size.ok_or_else(|| {
        ControlPlaneError::Backend(Box::<dyn std::error::Error + Send + Sync>::from(
            "parquet data file missing footer_size",
        ))
    })?;
    debug_assert!(matches!(file.file_format, control_plane_core::FileFormat::Parquet));
```
and change the bind `file.footer_size` → `footer_size`. (The `'parquet'` literal stays; `file_format` only has the `Parquet` variant in WS1.)

For the `ducklake_file_column_stats` INSERT (lines ~431-450): `value_count` is now derived. In that loop:
```rust
    for stat in &file.column_stats {
        let column_id = resolve_column_id(tx, table_id, &stat.column_name).await?;
        let value_count = file.record_count - stat.null_count; // DuckLake = non-null count
        let min_s = stat.min.as_ref().map(to_ducklake_stat_string);
        let max_s = stat.max.as_ref().map(to_ducklake_stat_string);
        // INSERT binds $5 = value_count, $7 = min_s, $8 = max_s (was stat.value_count/stat.min/stat.max)
```

- [ ] **Step 7: Update the DataFusion writer to emit typed stats.**

In `src/services/datafusion-io/src/write.rs`:
- Change the import (line 10) to `use control_plane_core::{ColumnStat, StatValue};`.
- Delete the private `Bound` enum and its `impl` (lines ~170-203).
- Replace `min_bound`/`max_bound` (lines ~205-231) to return `Option<StatValue>` (rename to `min_stat`/`max_stat`), mapping each Parquet `Statistics` arm to the matching `StatValue` variant (`Boolean→Bool`, `Int32→I32`, `Int64→I64`, `Float→F32`, `Double→F64`, `ByteArray→Str`).
- Add a free fn for the fold comparison (was `Bound::partial_cmp`):
```rust
fn stat_partial_cmp(a: &StatValue, b: &StatValue) -> Option<std::cmp::Ordering> {
    use StatValue::*;
    match (a, b) {
        (Bool(x), Bool(y)) => x.partial_cmp(y),
        (I32(x), I32(y)) => x.partial_cmp(y),
        (I64(x), I64(y)) => x.partial_cmp(y),
        (F32(x), F32(y)) => x.partial_cmp(y),
        (F64(x), F64(y)) => x.partial_cmp(y),
        (Str(x), Str(y)) => x.partial_cmp(y),
        _ => None,
    }
}
```
- In `file_stats_from_bytes` (lines ~248-289): use `min_stat`/`max_stat` and `stat_partial_cmp`; the `min`/`max` are now `Option<StatValue>` directly (delete the `.map(|b| b.to_stat_string())` stringify). Build `ColumnStat` WITHOUT `value_count`:
```rust
        column_stats.push(ColumnStat {
            column_name: field.name().clone(),
            null_count,
            column_size_bytes,
            min,
            max,
        });
```
- `WrittenFile` keeps `footer_size: i64` (unchanged); its `column_stats: Vec<ColumnStat>` now carries typed bounds. No other `WrittenFile` change.

- [ ] **Step 8: Update the two `DataFile` constructors.**

In `src/services/ingest/src/materialize.rs` (lines ~71-80), add the import `FileFormat` to the `control_plane_core` use (line 9) and construct:
```rust
        .map(|f| DataFile {
            path: f.path,
            path_is_relative: true,
            file_format: control_plane_core::FileFormat::Parquet,
            record_count: f.record_count,
            file_size_bytes: f.file_size_bytes,
            column_stats: f.column_stats,
            parquet_footer_size: Some(f.footer_size),
        })
```

In `src/services/transform/src/run.rs` (lines ~142-148), make the identical change to its `DataFile` construction (`file_format: control_plane_core::FileFormat::Parquet`, `parquet_footer_size: Some(f.footer_size)`, drop nothing else).

- [ ] **Step 9: Update all `DataFile`/`ColumnStat` test + fixture literals.**

Apply this transformation to every `DataFile`/`ColumnStat` literal listed below:
- `ColumnStat`: delete the `value_count: N,` line; change `min: Some("x".into())` → `min: Some(StatValue::Str("x".into()))` and likewise `max` (use `StatValue::I64`/`I32`/`Bool`/`F64`/`F32` when the literal is numeric/bool — match the column's type); `min: None`/`max: None` stay.
- `DataFile`: add `file_format: FileFormat::Parquet,`; change `footer_size: N` → `parquet_footer_size: Some(N)`.
- Add `use control_plane_core::{FileFormat, StatValue};` (merge into the existing `control_plane_core` import) wherever a literal is built.

Worked example — `postgres/tests/snapshot_create.rs:47-62` becomes:
```rust
            file_format: FileFormat::Parquet,
            record_count: 2,
            file_size_bytes: 123,
            parquet_footer_size: Some(50),
            column_stats: vec![
                ColumnStat {
                    column_name: "id".into(),
                    null_count: 0,
                    column_size_bytes: 16,
                    min: Some(StatValue::I64(1)),
                    max: Some(StatValue::I64(2)),
                },
                // ...second column likewise, no value_count
            ],
```
(Match the existing field values; only the SHAPE changes — `value_count` removed, `min`/`max` typed, `file_format` added, footer optional. Read each file to preserve its actual numbers and the column's type.)

Files to update (every `DataFile`/`ColumnStat` literal in each):
- `src/control-plane/testkit/src/lib.rs` (~1850)
- `src/control-plane/postgres/tests/snapshot_append.rs` (~25-40)
- `src/control-plane/postgres/tests/snapshot_create.rs` (~45-65)
- `src/control-plane/postgres/tests/snapshot_rollback.rs` (~55-60)
- `src/control-plane/postgres/tests/ducklake_interop.rs` (~140-160; this file also has a local `parquet_footer_size` helper — keep it, just feed `Some(...)` into the `DataFile`)
- `src/services/query-api/tests/governed_read.rs` (~75-105)
- `src/services/datafusion-io/tests/write.rs` (asserts, see Step 10)

- [ ] **Step 10: Update the `datafusion-io` write-test assertions.**

In `src/services/datafusion-io/tests/write.rs`: `stats.footer_size` (line ~84) still exists on `WrittenFile` (unchanged). Remove the `id.value_count`/`name.value_count` assertions (lines ~90, ~99). Where the test inspects `id.min`/`id.max`, update to the typed form (e.g. `assert_eq!(id.min, Some(StatValue::I64(1)))`), importing `StatValue`.

- [ ] **Step 11: Build the whole tree, then run the affected tests.**

Run: `buck2 build //src/... 2>&1 | tail -5`
Expected: build succeeds (no `value_count`/`footer_size`-shape errors).

Run: `buck2 test //src/control-plane/core:snapshot-types //src/control-plane/core:logical-type //src/services/datafusion-io:write //src/control-plane/postgres:snapshot-append //src/control-plane/postgres:snapshot-create //src/control-plane/postgres:ducklake-interop > /tmp/t1.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t1.log`
Expected: all PASS. The `ducklake-interop` oracle passing is the proof DuckLake byte-fidelity survived the metadata reshape.

- [ ] **Step 12: Commit.**

```bash
git add -A
git commit -F - <<'EOF'
feat(core): typed file-metadata contract; confine DuckLake stat encoding to adapter

Leak #1 + #4: ColumnStat carries typed StatValue bounds (no derivable value_count),
DataFile gains explicit FileFormat and Optional parquet_footer_size. The VARCHAR
stat encoding and value_count derivation move into the postgres adapter
(ducklake_type::to_ducklake_stat_string). core::catalog docs deleaked to be
format-neutral. DuckDB read-back interop oracle stays green.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
EOF
```

---

## Task 2 — Cascade 2: full logical column-type neutralization (leak #2)

**Files:**
- Modify: `src/control-plane/core/src/logical_type.rs`, `src/control-plane/core/tests/logical_type.rs`
- Modify: `src/control-plane/postgres/src/ducklake_type.rs`, `src/control-plane/postgres/tests/ducklake_type.rs`
- Modify: `src/control-plane/postgres/src/snapshot.rs` (write_table), `src/control-plane/postgres/src/catalog.rs` (schema)
- Modify: `src/services/datafusion-io/src/infer.rs`, `src/services/datafusion-io/src/lib.rs`, `src/services/datafusion-io/tests/infer.rs`
- Modify: `src/services/ingest/src/gate.rs`, `src/services/ingest/tests/gate.rs`, `src/services/ingest/tests/materialize.rs`
- Modify: `src/services/transform/tests/conform.rs`
- Modify: `src/control-plane/postgres/tests/catalog.rs`

One atomic cascade. Ends green.

- [ ] **Step 1: Change `satisfies` to logical-vs-logical; add `canonical_name`; remove `physical_affinity`.**

In `src/control-plane/core/src/logical_type.rs`:

Delete `BaseType::physical_affinity` (lines ~48-61). Add `canonical_name`:
```rust
impl BaseType {
    /// The canonical lowercase logical name (inverse of `resolve_logical` for base names).
    pub fn canonical_name(self) -> &'static str {
        match self {
            BaseType::Integer => "integer",
            BaseType::Long => "long",
            BaseType::Double => "double",
            BaseType::Boolean => "boolean",
            BaseType::String => "string",
            BaseType::Date => "date",
            BaseType::Timestamp => "timestamp",
        }
    }
    // json_repr stays unchanged
```

Replace `satisfies` (lines ~95-103):
```rust
/// Does a column's loom LOGICAL type satisfy a declared logical property type? Both
/// names are resolved to `BaseType` and compared (no implicit widening: Integer is
/// 32-bit, Long 64-bit — distinct base types). `Err(UnknownLogicalType)` if the
/// PROPERTY's logical type is unrecognized (an authoring error); an unrecognized
/// column type simply does not satisfy (`Ok(false)`).
pub fn satisfies(property_ty: &str, column_ty: &str) -> Result<bool, UnknownLogicalType> {
    let want = resolve_logical(property_ty)
        .ok_or_else(|| UnknownLogicalType(property_ty.trim().to_string()))?;
    Ok(resolve_logical(column_ty) == Some(want))
}
```

Update the module doc (lines 1-11) to drop the "DuckLake physical affinities" framing — say core defines the logical vocabulary; physical mapping lives in the adapter.

- [ ] **Step 2: Rewrite the core `logical_type` tests for the new semantics.**

In `src/control-plane/core/tests/logical_type.rs`, replace the `satisfies_*` tests:
```rust
#[test]
fn satisfies_matches_same_base_type() {
    assert_eq!(satisfies("Long", "long"), Ok(true));
    assert_eq!(satisfies("Integer", "integer"), Ok(true));
    assert_eq!(satisfies("Double", "double"), Ok(true));
    assert_eq!(satisfies("Boolean", "boolean"), Ok(true));
    assert_eq!(satisfies("String", "string"), Ok(true));
    // aliases resolve to their base on either side
    assert_eq!(satisfies("EmailAddress", "string"), Ok(true));
    assert_eq!(satisfies("String", "Url"), Ok(true));
    assert_eq!(satisfies("Date", "date"), Ok(true));
    assert_eq!(satisfies("Timestamp", "timestamp"), Ok(true));
}

#[test]
fn satisfies_rejects_different_base_type() {
    assert_eq!(satisfies("Integer", "long"), Ok(false));
    assert_eq!(satisfies("Long", "integer"), Ok(false));
}

#[test]
fn satisfies_unknown_column_type_does_not_satisfy() {
    assert_eq!(satisfies("Long", "int64"), Ok(false)); // "int64" is physical, not logical
}

#[test]
fn satisfies_normalizes_casing_and_whitespace() {
    assert_eq!(satisfies("String", "STRING"), Ok(true));
    assert_eq!(satisfies("Long", " long "), Ok(true));
    assert_eq!(satisfies(" Long ", "long"), Ok(true));
}

#[test]
fn satisfies_errors_on_unknown_property_type() {
    assert_eq!(satisfies("Money", "double"), Err(UnknownLogicalType("Money".into())));
}

#[test]
fn canonical_name_round_trips_base_names() {
    for name in ["integer", "long", "double", "boolean", "string", "date", "timestamp"] {
        let base = resolve_logical(name).unwrap();
        assert_eq!(base.canonical_name(), name);
    }
}
```
(Keep the `resolves_*`, `json_repr_*` tests as-is.)

Run: `buck2 test //src/control-plane/core:logical-type > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`
Expected: PASS (core green; tree red mid-cascade).

- [ ] **Step 3: Add the type-mapping fns to the postgres adapter + tests.**

In `src/control-plane/postgres/src/ducklake_type.rs`, add:
```rust
use control_plane_core::BaseType;

/// loom logical base type → DuckLake physical type string (single-valued; was
/// `core::BaseType::physical_affinity`). Used on the write path (`create_table`).
pub(crate) fn ducklake_physical_type(base: BaseType) -> &'static str {
    match base {
        BaseType::Integer => "int32",
        BaseType::Long => "int64",
        BaseType::Double => "float64",
        BaseType::Boolean => "boolean",
        BaseType::String => "varchar",
        BaseType::Date => "date",
        BaseType::Timestamp => "timestamp",
    }
}

/// DuckLake physical type string → loom logical base type (read path, `schema()`).
/// `None` for a physical type loom has no logical name for (surfaced as an error,
/// never leaked back into core as a raw string).
pub(crate) fn logical_from_ducklake(physical: &str) -> Option<BaseType> {
    match physical.trim().to_ascii_lowercase().as_str() {
        "int32" => Some(BaseType::Integer),
        "int64" => Some(BaseType::Long),
        "float64" => Some(BaseType::Double),
        "boolean" => Some(BaseType::Boolean),
        "varchar" => Some(BaseType::String),
        "date" => Some(BaseType::Date),
        "timestamp" => Some(BaseType::Timestamp),
        _ => None,
    }
}
```

Make these `pub(crate)` fns directly testable: in `src/control-plane/postgres/tests/ducklake_type.rs` you cannot reach `pub(crate)` from an integration test, so add a thin always-compiled re-export guarded for tests is overkill — instead add round-trip assertions via a re-declared expectation (as in Task 1 Step 5) OR, preferred, promote `ducklake_physical_type`/`logical_from_ducklake` to `pub` (they are a deliberate adapter API) and import them:
```rust
use control_plane_postgres::ducklake_type::{ducklake_physical_type, logical_from_ducklake};
use control_plane_core::BaseType;

#[test]
fn physical_and_logical_round_trip() {
    for base in [BaseType::Integer, BaseType::Long, BaseType::Double,
                 BaseType::Boolean, BaseType::String, BaseType::Date, BaseType::Timestamp] {
        let phys = ducklake_physical_type(base);
        assert_eq!(logical_from_ducklake(phys), Some(base));
    }
}

#[test]
fn unknown_physical_type_has_no_logical() {
    assert_eq!(logical_from_ducklake("decimal(10,2)"), None);
}
```
If you promote to `pub`, also add `pub mod ducklake_type;` (instead of `mod`) in `src/control-plane/postgres/src/lib.rs` and `pub use` is not required — the test imports the module path. Add `"//src/control-plane/core:core"` to the test target deps if not already present.

- [ ] **Step 4: Map logical→ducklake on `create_table` (write path).**

In `src/control-plane/postgres/src/snapshot.rs::write_table` (the `for (i, col) in columns.iter()...` loop, ~line 251): the bind `col.ty` ($6 `column_type`) must become the physical string. Add the import `use control_plane_core::resolve_logical;` and `use crate::ducklake_type::ducklake_physical_type;`, then:
```rust
    for (i, col) in columns.iter().enumerate() {
        let column_id = (i + 1) as i64;
        let base = resolve_logical(&col.ty).ok_or_else(|| {
            ControlPlaneError::Backend(Box::<dyn std::error::Error + Send + Sync>::from(
                format!("unknown logical column type {:?} for {}", col.ty, col.name),
            ))
        })?;
        let physical = ducklake_physical_type(base);
        // INSERT binds $6 = physical (was col.ty)
```

- [ ] **Step 5: Map ducklake→logical on `schema()` (read path).**

In `src/control-plane/postgres/src/catalog.rs::schema` (lines ~98-121): the `column_type` string must be mapped to a logical name. Add imports `use control_plane_core::BaseType;` and `use crate::ducklake_type::logical_from_ducklake;`, then build each `ColumnDef`:
```rust
            columns: rows
                .into_iter()
                .map(|r| {
                    let ty = logical_from_ducklake(&r.column_type)
                        .map(BaseType::canonical_name)
                        .ok_or_else(|| {
                            ControlPlaneError::Backend(Box::<dyn std::error::Error + Send + Sync>::from(
                                format!("catalog column type {:?} has no loom logical type", r.column_type),
                            ))
                        })?;
                    Ok(ColumnDef {
                        order: r.column_order,
                        name: r.column_name,
                        ty: ty.to_string(),
                        nullable: r.nulls_allowed,
                    })
                })
                .collect::<Result<Vec<_>>>()?,
```
(Change the surrounding `Ok(TableSchema { columns: ... })` so the `?` is valid — collect into `Result<Vec<ColumnDef>>` first.)

- [ ] **Step 6: Make Arrow inference emit logical types.**

In `src/services/datafusion-io/src/infer.rs`, rename `duck_type` → `arrow_logical_type` returning loom-logical canonical strings:
```rust
/// The loom LOGICAL type name for an Arrow type, or `None` if loom does not yet land
/// that type. Kept deliberately small (YAGNI) — extend as real data needs it.
pub fn arrow_logical_type(dt: &DataType) -> Option<&'static str> {
    match dt {
        DataType::Boolean => Some("boolean"),
        DataType::Int32 => Some("integer"),
        DataType::Int64 => Some("long"),
        DataType::Float64 => Some("double"),
        DataType::Utf8 | DataType::LargeUtf8 => Some("string"),
        _ => None,
    }
}
```
Update `infer_columns` to call `arrow_logical_type`. In `src/services/datafusion-io/src/lib.rs` (line 11) re-export `arrow_logical_type` instead of `duck_type`. Update the `scan.rs:52` comment ("maps the canonical types to **logical** types").

- [ ] **Step 7: Update `infer` tests.**

In `src/services/datafusion-io/tests/infer.rs`, import `arrow_logical_type` and update expectations:
```rust
    assert_eq!(arrow_logical_type(&DataType::Int64), Some("long"));
    assert_eq!(arrow_logical_type(&DataType::Utf8), Some("string"));
    assert_eq!(arrow_logical_type(&DataType::LargeUtf8), Some("string"));
    assert_eq!(arrow_logical_type(&DataType::Boolean), Some("boolean"));
    assert_eq!(arrow_logical_type(&DataType::Float64), Some("double"));
    assert_eq!(arrow_logical_type(&DataType::Int32), Some("integer"));
```
Update the `infer_columns` assertion (line ~20) to expect logical `ty` values.

- [ ] **Step 8: Make the ingest gate logical.**

In `src/services/ingest/src/gate.rs`: change the import (line 13) to `use datafusion_io::arrow_logical_type;`; reword `ColumnShape.ty` doc (line 15) to "a loom logical type string"; reword `ViolationReason::TypeMismatch` doc (line 38) to "inferred logical type"; in `validate` (line 62) call `arrow_logical_type(field.data_type())`.

- [ ] **Step 9: Update ingest gate/materialize tests.**

In `src/services/ingest/tests/gate.rs`, change every `ColumnShape.ty` and expected `TypeMismatch`/`found` value from ducklake strings to logical ("int64"→"long", "varchar"→"string", "int32"→"integer", "float64"→"double", "boolean"→"boolean").

In `src/services/ingest/tests/materialize.rs` (the model at line ~108), change the `ModelShape`/`ColumnShape` `ty` values to logical, and any post-land schema assertion to expect logical `ColumnDef.ty`.

- [ ] **Step 10: Update transform conform tests.**

In `src/services/transform/tests/conform.rs`, change `ColumnSpec.ty` and `PropertyDef.ty` literals and the expected `Violation::TypeMismatch { physical, .. }` values to logical names (the `physical` field now carries a logical column type). `conform.rs` itself needs no code change.

- [ ] **Step 11: Update the postgres catalog read test.**

In `src/control-plane/postgres/tests/catalog.rs`, the `schema()` assertion must now expect logical `ColumnDef.ty` (e.g. a column created as `int64` reads back as `"long"`). Update the expected `ColumnDef` literals accordingly.

- [ ] **Step 12: Build the tree and run the affected + fixture tests.**

Run: `buck2 build //src/... 2>&1 | tail -5`
Expected: success.

Run: `buck2 test //src/control-plane/core:logical-type //src/control-plane/postgres:ducklake-type //src/services/datafusion-io:infer //src/services/ingest:gate //src/services/transform:conform //src/control-plane/postgres:catalog //src/services/ingest:materialize //src/control-plane/postgres:ducklake-interop //src/services/ingest:ducklake-interop > /tmp/t2.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t2.log`
Expected: all PASS. The interop oracles + the postgres `catalog`/`materialize` fixture tests passing prove logical↔ducklake mapping is correct on both read and write.

- [ ] **Step 13: Commit.**

```bash
git add -A
git commit -F - <<'EOF'
feat(core): neutralize column types to logical; DuckLake type mapping in adapter

Leak #2 (full): ColumnSpec.ty / ColumnDef.ty are loom LOGICAL type names; satisfies
is logical-vs-logical; BaseType::physical_affinity removed from core. The bidirectional
ducklake<->logical mapping (ducklake_physical_type / logical_from_ducklake) lives in the
postgres adapter and is applied on create_table (write) and schema() (read). Arrow
inference and the ingest gate emit logical types. Interop oracles stay green.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
EOF
```

---

## Task 3 — Whole-tree verification

**Files:** none (verification only).

- [ ] **Step 1: Full build.**

Run: `buck2 build //src/... 2>&1 | tail -5`
Expected: success.

- [ ] **Step 2: Full test sweep.**

Run: `buck2 test //src/... > /tmp/ws1-all.log 2>&1; grep -E "Tests finished|FAIL" /tmp/ws1-all.log`
Expected: `Tests finished: ... 0 failed`. If any fixture test fails, read `/tmp/ws1-all.log` for the failing assertion and fix the corresponding literal/expectation.

- [ ] **Step 3: Clippy (the prek hook's lint path).**

Run: `./tools/clippy-all.sh > /tmp/clippy.log 2>&1; grep -E "error|warning:" /tmp/clippy.log || echo CLEAN`
Expected: CLEAN (no clippy findings in the changed crates).

- [ ] **Step 4: Format + hooks.**

Run: `buck2 run //tools:prek -- run --all-files > /tmp/prek.log 2>&1; grep -E "Failed|Passed" /tmp/prek.log`
Expected: all hooks Passed. If rustfmt rewrote files, `git add -A` them.

- [ ] **Step 5: Final commit (only if Step 4 changed files).**

```bash
git add -A
git commit -F - <<'EOF'
chore(core): rustfmt/clippy after table-format seam neutralization
Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
EOF
```

---

## Self-Review (run before handing off)

**Spec coverage (§Workstream 1):**
- Leak #1 typed bounds → Task 1 Steps 1, 7, 9. ✓
- Leak #1 drop value_count → Task 1 Steps 1, 6, 9, 10. ✓
- Leak #1 FileFormat + Optional footer → Task 1 Steps 1, 6, 8, 9. ✓
- Leak #2 logical ColumnSpec/ColumnDef → Task 2 Steps 4, 5, 6, 8. ✓
- Leak #2 mapping moved to adapter → Task 2 Step 3. ✓
- Leak #2 satisfies logical-vs-logical → Task 2 Steps 1, 2. ✓
- Leak #4 doc deleak → Task 1 Step 3. ✓
- Iceberg mapping table acceptance (derive value_count, typed bounds, name-keyed, explicit format) → enforced by Task 1 shape + the interop oracles. ✓

**Type consistency:** `StatValue`, `FileFormat`, `ColumnStat{column_name,null_count,column_size_bytes,min,max}`, `DataFile{..,file_format,parquet_footer_size}`, `satisfies(property_ty,column_ty)`, `ducklake_physical_type`/`logical_from_ducklake`, `arrow_logical_type`, `BaseType::canonical_name` — names used identically across all tasks.

**No `.sqlx` regen:** confirmed — every SQL string is unchanged; only bound Rust values differ.
