# road-value-bridge Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** One authoritative PG↔Arrow conversion layer. The postgres adapter's
`column_array` becomes the single PG-row → Arrow decode (exported, `BaseType`-typed);
engine-serving's drifted copies (`pg_rows_to_arrays`, `base_to_arrow`) are deleted;
the `"element"`/`"item"` Arrow list-child-name divergence collapses to **`"item"`**;
`PgTableProvider.logical_types` becomes `Vec<BaseType>` resolved at construction; and
a single `BaseType → DataType` map lands in core, consulted by every conversion site
(postgres adapter, engine-serving, query-api's flight export, datafusion-io's infer).
Fixes `iss-pg-provider-vector-drift`: a SQL read projecting a vector column of a
table with live inline rows currently fails at scan time with
`inline PG provider: unsupported logical type "vector(4)"`.

**Architecture:**

- **The map lives in core** (`logical_type.rs`, next to `BaseType`/`resolve_logical`),
  as the spec leans: core already owns the closed logical-type vocabulary, and *four*
  crates need the map — postgres adapter, engine-serving, query-api (flight export),
  datafusion-io — of which query-api deliberately does **not** depend on the postgres
  adapter or DataFusion, so the adapter cannot host it without either duplicating it
  again or adding a heavyweight dep. Core gains only `arrow-schema = "58"` (type
  definitions, no arrays/IPC/I/O — the "No I/O" charter holds; the tree is on a single
  arrow major, 58, so `arrow_schema::DataType` is type-identical to
  `arrow::datatypes::DataType` in every consumer). The existing note in
  `logical_type.rs` that "physical mapping lives in the postgres adapter" refers to
  Iceberg *type strings* (`iceberg_type.rs`), which stay in the adapter.
- **The decode seam is `pub fn column_array` in `control_plane_postgres::iceberg_inline`**
  — no new `arrow_bridge` module. engine-serving already imports two pub fns from that
  module (`has_live_inline_rows`, `inline_table_name`); a third fits the established
  seam, and the fn stays next to its write-direction sibling (`cell_from_arrow`, which
  is deliberately out of scope — `fut-coercion-taxonomy`). Its signature flips from
  `&str` logical names to `BaseType`, making the match exhaustive (a new `BaseType`
  member becomes a compile error at every decode site, never a mid-scan
  "unsupported logical type").
- **List child name: `"item"` wins.** Evidence: (1) `"item"` is arrow-rs's own default
  (`ListBuilder`, `Field::new_list_field`), so every wire client and all 18+ existing
  loom construction sites (`iceberg_inline::arrow_field`/`column_array`,
  `vector_index.rs`, and every test helper in postgres/engine-serving/engine/worker/
  query-api tests) already say `"item"`; the only `"element"` sites are the two
  drifted copies being deleted (`serving.rs::base_to_arrow`,
  `flight_export.rs::base_to_arrow`). (2) **No test anywhere asserts `"element"`** —
  `export_command.rs::export_schema_maps_scalars_and_vector` asserts the child's
  *datatype and nullability* only; `governed_flight_export_e2e` matches
  `DataType::List(_)` — so the rename breaks no pinned assertion, whereas
  standardizing on `"element"` would break the postgres adapter's inline round-trip
  suites and fight arrow defaults on the ingest wire forever. (3) The one place
  `"element"` is *required* — Iceberg's Parquet file schema — is already handled as a
  deliberate storage-boundary relabel at write time
  (`iceberg_landing.rs::coerce_batch_to_ice` rebuilds the list under Iceberg's
  `element` + `PARQUET:field_id` child); at read time DataFusion's default schema
  adapter casts `List` → `List` by child *type*, not name. The file-backed arm of the
  new e2e test proves that adaptation empirically (it is the one behavior the rename
  could regress).
- **Blast radius of the rename:** the mirror-derived serving schema
  (`arrow_schema_from_mirror`) flips `element` → `item`, which changes (a) what the
  file provider presents for vector columns (data on disk keeps `element`; adapter
  casts), (b) the inline provider's declared schema — which is exactly what makes the
  shared `column_array` output (`item`) satisfy `RecordBatch::try_new`'s strict
  nested-field equality, and (c) query-api's `get_flight_info` advertised export
  schema, which must flip in the same PR because `flight_export.rs` documents a
  lockstep contract with what the engine streams. All three flip within this
  PR; note Task 3's commit flips the engine stream one commit before Task 4
  flips flight_export's advertised schema — verified safe, no test pins the
  list-child name (export_command.rs asserts child type/nullability only).

**Tech Stack:** Rust (edition 2024), buck2, `loom_fixture_test` (hermetic Postgres),
arrow 58, DataFusion 54, reindeer (`./tools/buckify.sh`) for the one Cargo.toml change.

## Global Constraints

- Spec: `docs/superpowers/specs/2026-07-02-pillar-idioms-audit-design.md`
  (§ "road-value-bridge"); registers: `docs/ROADMAP.md#road-value-bridge`,
  `docs/ISSUES.md#iss-pg-provider-vector-drift`.
- **TDD:** every behavior change lands with its test written first and observed red
  (Task 3 step 2 is the mandated failing read of a vector column over live inline rows).
- Tests are separate `rust_test`/`loom_fixture_test` targets, never inline
  `#[cfg(test)]`. New fixture tests MUST use `loom_fixture_test`.
- **Never pipe `buck2 test`/`buck2 bxl` through `tail`/`head`** — redirect to a file
  and grep it. Multi-fixture runs use `-j 8` (postgres boot-slot starvation otherwise).
- `buck2 run //tools:prek -- run --all-files` must report zero `Failed` before **every**
  commit; conventional-commit message format.
- One-unsafe-op-per-block: N/A (no `unsafe` anywhere in this change).
- **`.sqlx` untouched expected** — no compile-time `query!` SQL changes anywhere in
  this plan; if `src/control-plane/postgres/.sqlx/` shows a diff, something went wrong.
- Cargo.toml changes (core gains `arrow-schema`) require `eval "$(./tools/env.sh)"`,
  `cargo generate-lockfile`, `./tools/buckify.sh`; then **diff `Cargo.lock` against
  the merge-base for native/`links` crates (`zstd-sys`, `ring`)** — `generate-lockfile`
  re-resolves the whole graph (CLAUDE.md guard). `third-party/BUCK` should be
  byte-identical (arrow-schema is already a resolved target).

## Behavior pinned by existing tests (must NOT change)

- `//src/control-plane/postgres:iceberg-inline-vector` — inline vector round-trip is
  bit-exact and the reconstructed list child is `"item"` (constructed and compared via
  full nested-field equality in `RecordBatch::try_new`). The chosen name keeps this green.
- `//src/services/query-api:export-command` (`export_schema_maps_scalars_and_vector`)
  — vector advertises as `List` of non-null `Float32`; **child name deliberately not
  asserted**. Masked columns advertise `Utf8`. Both preserved.
- `//src/services/datafusion-io:logical-arrow` — `logical_arrow_type("date") == None`
  pins the infer map's deliberate narrowness; the delegation in Task 4 keeps the
  five-name allowlist.
- `//src/services/engine-serving:pg-scan-sql` — `build_scan_sql` is untouched
  (signature and output identical).
- `//src/services/engine-serving:execute-query-e2e` and
  `//src/services/query-api:datafusion-inline-union` — primitive file+inline union
  reads; the primitive decode arms are moved, not changed.
- `//src/services/query-api:wire-harness-smoke`/`wire_governed_read_e2e` — query-api's
  `Rows.logical_types: Vec<String>` wire shape is NOT part of this change; only the
  engine-serving *provider's* internal field changes type.

---

### Task 1: Core authoritative `BaseType → DataType` map

**Files:**
- Modify: `src/control-plane/core/Cargo.toml` (add `arrow-schema = "58"`)
- Modify: `src/control-plane/core/BUCK` (`:core` deps + `:logical-type` test deps)
- Modify: `src/control-plane/core/src/logical_type.rs` (the map + `vector_list_field`)
- Modify: `src/control-plane/core/src/lib.rs:49` (re-export `vector_list_field`)
- Modify: `Cargo.lock` (regenerated), `third-party/BUCK` (expected no diff)
- Test: `src/control-plane/core/tests/logical_type.rs` (target `//src/control-plane/core:logical-type`)

**Interfaces:**
- Produces: `pub fn BaseType::arrow_data_type(self) -> arrow_schema::DataType` and
  `pub fn vector_list_field() -> arrow_schema::Field` (re-exported from the crate root).
- Consumed by Tasks 2–4 (postgres adapter, engine-serving, query-api, datafusion-io).

- [ ] **Step 1: Write the failing test**

Append to `src/control-plane/core/tests/logical_type.rs` (extend the existing
`use control_plane_core::{…}` line with `vector_list_field`):

```rust
#[test]
fn arrow_data_type_maps_every_base_type() {
    use arrow_schema::{DataType, TimeUnit};
    assert_eq!(BaseType::Integer.arrow_data_type(), DataType::Int32);
    assert_eq!(BaseType::Long.arrow_data_type(), DataType::Int64);
    assert_eq!(BaseType::Double.arrow_data_type(), DataType::Float64);
    assert_eq!(BaseType::Boolean.arrow_data_type(), DataType::Boolean);
    assert_eq!(BaseType::String.arrow_data_type(), DataType::Utf8);
    assert_eq!(BaseType::Date.arrow_data_type(), DataType::Date32);
    assert_eq!(
        BaseType::Timestamp.arrow_data_type(),
        DataType::Timestamp(TimeUnit::Microsecond, None)
    );
}

#[test]
fn vector_arrow_type_is_list_of_item_float32() {
    use arrow_schema::DataType;
    // The list child is named "item" (arrow-rs's default; loom's wire/in-memory
    // convention). Iceberg's Parquet storage relabels to "element" at the write
    // boundary only (coerce_batch_to_ice) — never in an in-memory schema.
    match BaseType::Vector(4).arrow_data_type() {
        DataType::List(f) => {
            assert_eq!(f.name(), "item");
            assert_eq!(f.data_type(), &DataType::Float32);
            assert!(!f.is_nullable());
        }
        other => panic!("expected List, got {other:?}"),
    }
    assert_eq!(vector_list_field().name(), "item");
}
```

Add to the `logical-type` test target's deps in `src/control-plane/core/BUCK`:
`"//third-party:arrow-schema",`.

- [ ] **Step 2: Run test to verify it fails**

Run: `buck2 test //src/control-plane/core:logical-type > /tmp/t1.log 2>&1; grep -E "Tests finished|FAIL|error" /tmp/t1.log`
Expected: FAIL — compile error (`arrow_data_type`/`vector_list_field` don't exist).

- [ ] **Step 3: Add the dep and the map**

`src/control-plane/core/Cargo.toml`, under `[dependencies]`:

```toml
# Arrow physical mapping for BaseType (arrow_data_type). Schema types only — no
# arrays, no IPC, no I/O; the whole tree is on a single arrow major (58).
arrow-schema = "58"
```

Then regenerate and verify (hermetic toolchain):

```bash
eval "$(./tools/env.sh)"
cargo generate-lockfile
./tools/buckify.sh
git diff --stat third-party/BUCK          # expected: empty
git diff origin/main -- Cargo.lock | grep -E "zstd-sys|ring"  # expected: empty
```

Add `"//third-party:arrow-schema",` to the `:core` `rust_library` deps in
`src/control-plane/core/BUCK` (alphabetically first in the list).

In `src/control-plane/core/src/logical_type.rs`: update the module doc's physical-
mapping sentence to:

```rust
//! authoritative. Physical mappings: logical ↔ Iceberg type strings live in the
//! postgres adapter (`control_plane_postgres::iceberg_type`); logical → Arrow
//! `DataType` lives HERE (`BaseType::arrow_data_type`) as the single map every
//! conversion site consults (adapter inline reads, engine serving, flight export,
//! ingest inference).
```

and add (below the `impl BaseType` block's `json_repr`):

```rust
/// The Arrow list-child field of every loom `Vector(N)` column, in memory and on
/// the wire: `"item"`, non-null `Float32`. `"item"` is arrow-rs's own default
/// (`ListBuilder`, `Field::new_list_field`), so wire batches match without
/// relabeling. Iceberg's Parquet storage uses `"element"` (+ `PARQUET:field_id`);
/// that relabel happens ONLY at the storage write boundary
/// (`iceberg_landing::coerce_batch_to_ice`) — DataFusion's schema adapter casts
/// the name back on read.
pub fn vector_list_field() -> arrow_schema::Field {
    arrow_schema::Field::new("item", arrow_schema::DataType::Float32, false)
}
```

and inside `impl BaseType`:

```rust
    /// THE authoritative loom-logical → Arrow (58) physical mapping. Every
    /// conversion site (postgres adapter reads, engine serving schemas, query-api
    /// flight export, datafusion-io inference) consults this — never a local copy
    /// (see iss-pg-provider-vector-drift for what a drifted copy cost). Canonical,
    /// non-`*View` variants, so decoded values match the file-Parquet side.
    pub fn arrow_data_type(self) -> arrow_schema::DataType {
        use arrow_schema::{DataType, TimeUnit};
        match self {
            BaseType::Integer => DataType::Int32,
            BaseType::Long => DataType::Int64,
            BaseType::Double => DataType::Float64,
            BaseType::Boolean => DataType::Boolean,
            BaseType::String => DataType::Utf8,
            BaseType::Date => DataType::Date32,
            BaseType::Timestamp => DataType::Timestamp(TimeUnit::Microsecond, None),
            BaseType::Vector(_) => {
                DataType::List(std::sync::Arc::new(vector_list_field()))
            }
        }
    }
```

In `src/control-plane/core/src/lib.rs`, extend the `pub use logical_type::{…}`
list (line 49) with `vector_list_field`.

- [ ] **Step 4: Run tests + clippy**

Run: `buck2 test //src/control-plane/core:logical-type > /tmp/t1.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t1.log`
Expected: PASS.
Run: `buck2 build '//src/control-plane/core:core[clippy.txt]' > /tmp/c1.log 2>&1` and check the artifact is empty.

- [ ] **Step 5: prek + commit**

Run: `buck2 run //tools:prek -- run --all-files > /tmp/p1.log 2>&1; grep -c Failed /tmp/p1.log` — expected `0`.

```bash
git add src/control-plane/core Cargo.lock docs/superpowers/plans/2026-07-02-value-bridge.md
git commit -m "feat(core): authoritative BaseType -> Arrow DataType map

One map, next to BaseType/resolve_logical, that every conversion site
consults (postgres adapter inline reads, engine serving schemas, query-api
flight export, datafusion-io inference). Vector columns are List<Float32>
with child field \"item\" — arrow-rs's default and what every loom builder
already produces; Iceberg's \"element\" stays a storage-boundary relabel.
core gains only arrow-schema (types, no arrays/IPC/I/O).

Part of road-value-bridge."
```

---

### Task 2: Export the postgres adapter's decode (`column_array`, `BaseType`-typed)

**Files:**
- Modify: `src/control-plane/postgres/src/iceberg_inline.rs:349-459` (`arrow_field`,
  `column_array`), `:461-533` (`inline_live_batch`), imports (`:11-25`)
- Test targets (existing, must stay green): `//src/control-plane/postgres:iceberg-inline`,
  `:iceberg-inline-vector`, `:iceberg-inline-types`, `:iceberg-flush`,
  `:flush-vector-rebuild`, `:vector-landing`

**Interfaces:**
- Produces: `pub fn column_array(rows: &[sqlx::postgres::PgRow], i: usize, ty: BaseType) -> Result<ArrayRef>`
  (was private, `&str`-typed). `arrow_field` stays private but becomes infallible:
  `fn arrow_field(name: &str, ty: BaseType, nullable: bool) -> Field`.
- Consumes: `BaseType`, `resolve_logical`, `vector_list_field` from core (Task 1).
- Behavior invariant: an unrecognized mirror logical type still fails
  `inline_live_batch` with `ControlPlaneError::Backend("inline read: unsupported type …")`
  — the error just moves from the per-column match arm to a single resolve step.

- [ ] **Step 1: Retype `arrow_field` and `column_array`**

Replace both functions in `src/control-plane/postgres/src/iceberg_inline.rs`:

```rust
/// Arrow field for a logical column. Delegates to core's authoritative
/// `BaseType::arrow_data_type` map (canonical, non-`*View`, vector list child
/// `"item"`), so the inline read schema can never drift from the serving path.
fn arrow_field(name: &str, ty: BaseType, nullable: bool) -> Field {
    Field::new(name, ty.arrow_data_type(), nullable)
}

/// Build an arrow array for positional column `i` (typed `ty`) from PG rows.
///
/// THE single PG-row → Arrow decode: the inline read path (`inline_live_batch`)
/// and engine-serving's `PgTableProvider` both call this, so a new `BaseType`
/// member is a compile error here — never a silent "unsupported logical type"
/// at scan time (iss-pg-provider-vector-drift).
pub fn column_array(rows: &[sqlx::postgres::PgRow], i: usize, ty: BaseType) -> Result<ArrayRef> {
    macro_rules! get {
        ($ty:ty) => {
            rows.iter()
                .map(|r| r.try_get::<Option<$ty>, _>(i))
                .collect::<std::result::Result<Vec<_>, _>>()
                .map_err(backend)?
        };
    }
    Ok(match ty {
        BaseType::Integer => {
            let mut b = Int32Builder::new();
            for v in get!(i32) {
                b.append_option(v);
            }
            Arc::new(b.finish())
        }
        BaseType::Long => {
            let mut b = Int64Builder::new();
            for v in get!(i64) {
                b.append_option(v);
            }
            Arc::new(b.finish())
        }
        BaseType::Double => {
            let mut b = Float64Builder::new();
            for v in get!(f64) {
                b.append_option(v);
            }
            Arc::new(b.finish())
        }
        BaseType::Boolean => {
            let mut b = BooleanBuilder::new();
            for v in get!(bool) {
                b.append_option(v);
            }
            Arc::new(b.finish())
        }
        BaseType::String => {
            let mut b = StringBuilder::new();
            for v in get!(String) {
                b.append_option(v);
            }
            Arc::new(b.finish())
        }
        BaseType::Date => {
            let mut b = Date32Builder::new();
            let epoch = time::macros::date!(1970 - 01 - 01);
            for v in get!(time::Date) {
                b.append_option(v.map(|d| (d - epoch).whole_days() as i32));
            }
            Arc::new(b.finish())
        }
        BaseType::Timestamp => {
            let mut b = TimestampMicrosecondBuilder::new();
            for v in get!(time::PrimitiveDateTime) {
                b.append_option(v.map(|t| {
                    (t.assume_utc() - time::OffsetDateTime::UNIX_EPOCH)
                        .whole_microseconds()
                        .try_into()
                        .unwrap_or(i64::MAX)
                }));
            }
            Arc::new(b.finish())
        }
        BaseType::Vector(_) => {
            use arrow_array::builder::{Float32Builder, ListBuilder};
            let item = Arc::new(control_plane_core::vector_list_field());
            let mut b = ListBuilder::new(Float32Builder::new()).with_field(item);
            for xs in get!(Vec<f32>) {
                match xs {
                    Some(xs) => {
                        b.values().append_slice(&xs);
                        b.append(true);
                    }
                    None => b.append(false),
                }
            }
            Arc::new(b.finish())
        }
    })
}
```

The match is exhaustive — the `other => Err(Backend(…))` arms of both old fns are
deleted. (The numeric casts on the date arm are covered by the toolchain-wide
`CLIPPY_ALLOWS`; no per-site attributes exist or are needed.)

- [ ] **Step 2: Resolve once in `inline_live_batch`**

In `IcebergCatalog::inline_live_batch` (`iceberg_inline.rs:516-527`), replace the
field/array construction:

```rust
        // Resolve every column's logical type ONCE (the mirror should never hold
        // an unrecognized one) — same failure text the per-column arms used to emit.
        let types: Vec<BaseType> = schema
            .columns
            .iter()
            .map(|c| {
                resolve_logical(&c.ty).ok_or_else(|| {
                    ControlPlaneError::Backend(
                        format!("inline read: unsupported type {:?}", c.ty).into(),
                    )
                })
            })
            .collect::<Result<Vec<_>>>()?;

        // Build arrow arrays per column. `column_array` indexes positional columns;
        // the data columns start at index 1 (loom_row_id is column 0), so pass `i + 1`.
        let fields: Vec<Field> = schema
            .columns
            .iter()
            .zip(&types)
            .map(|(c, ty)| arrow_field(&c.name, *ty, c.nullable))
            .collect();
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(fields.len());
        for (i, ty) in types.iter().enumerate() {
            arrays.push(column_array(&rows, i + 1, *ty)?);
        }
```

Imports: extend the `control_plane_core::{…}` import with `BaseType, resolve_logical`;
remove `DataType` and `TimeUnit` from the `arrow_schema::{…}` import (now unused —
`Field` and `Schema` stay). The build (clippy gate = zero warnings) will confirm.

- [ ] **Step 3: Run the adapter suites**

Run: `buck2 test -j 8 //src/control-plane/postgres:iceberg-inline //src/control-plane/postgres:iceberg-inline-vector //src/control-plane/postgres:iceberg-inline-types //src/control-plane/postgres:iceberg-flush //src/control-plane/postgres:flush-vector-rebuild //src/control-plane/postgres:vector-landing > /tmp/t2.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t2.log`
Expected: PASS (pure refactor: same decode, same `"item"` child, same error text).

- [ ] **Step 4: Clippy + prek + commit**

Run: `buck2 build '//src/control-plane/postgres:postgres[clippy.txt]' > /tmp/c2.log 2>&1` — artifact empty.
Run: `buck2 run //tools:prek -- run --all-files > /tmp/p2.log 2>&1; grep -c Failed /tmp/p2.log` — expected `0`.
Verify: `git status src/control-plane/postgres/.sqlx` — untouched.

```bash
git add src/control-plane/postgres/src/iceberg_inline.rs
git commit -m "refactor(iceberg): export column_array as the single PG -> Arrow decode

arrow_field/column_array flip from &str logical names to BaseType (exhaustive
match — a new BaseType member is now a compile error at the decode, never a
mid-scan \"unsupported logical type\") and consult core's authoritative
arrow_data_type map; column_array goes pub so engine-serving's PgTableProvider
can share it instead of keeping the drifted copy. inline_live_batch resolves
logical types once up front, preserving the Backend error text.

Part of road-value-bridge."
```

---

### Task 3: engine-serving on the shared bridge (TDD — fixes iss-pg-provider-vector-drift)

**Files:**
- Create: `src/services/engine-serving/tests/inline_vector_sql.rs`
- Modify: `src/services/engine-serving/BUCK` (new `loom_fixture_test` target)
- Modify: `src/services/engine-serving/src/provider.rs` (delete `pg_rows_to_arrays`;
  `logical_types: Vec<BaseType>`)
- Modify: `src/services/engine-serving/src/serving.rs` (delete `base_to_arrow`;
  `arrow_schema_from_mirror` + `build_inline_provider` on the shared map)

**Interfaces:**
- Consumes: `control_plane_postgres::iceberg_inline::column_array` (Task 2),
  `BaseType::arrow_data_type` (Task 1).
- Produces: `PgTableProvider::new(pool, relation, schema, logical_types: Vec<BaseType>, base_filter)`
  — signature change; the only constructor call site is `build_inline_provider`
  (verified: no other `PgTableProvider::new` callers in the tree).
- `build_scan_sql` untouched (`//src/services/engine-serving:pg-scan-sql` stays green).

- [ ] **Step 1: Write the failing e2e test**

Create `src/services/engine-serving/tests/inline_vector_sql.rs`. Harness helpers are
copied from the sibling `tests/vector_search.rs` (per-file harness divergence is the
norm in this crate):

```rust
//! Plain SQL reads projecting a `vector(N)` column through `execute_query`, across
//! all three storage arms: Parquet files (pins the file path across the
//! element→item serving-schema rename), live inline PG rows (the
//! iss-pg-provider-vector-drift defect — RED until PgTableProvider shares the
//! adapter's column_array), and the files∪inline UNION.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::builder::{Float32Builder, ListBuilder};
use arrow_array::{Float32Array, Int64Array, ListArray, RecordBatch};
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{DataType, Field, Schema};
use control_plane_core::{
    ColumnSpec, DatasetId, EventType, LineageEvent, RunId, TableRef,
};
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use control_plane_postgres::iceberg_landing::land;
use control_plane_postgres::iceberg_sql_catalog::{
    SQL_CATALOG_PROP_URI, SQL_CATALOG_PROP_WAREHOUSE, SqlCatalog, SqlCatalogBuilder,
};
use iceberg::CatalogBuilder;
use iceberg::io::LocalFsStorageFactory;

fn columns() -> Vec<ColumnSpec> {
    vec![
        ColumnSpec {
            name: "id".into(),
            ty: "long".into(),
            nullable: false,
        },
        ColumnSpec {
            name: "embedding".into(),
            ty: "vector(4)".into(),
            nullable: false,
        },
    ]
}

/// Arrow IPC body with `id: long` + `embedding: list<float32>` (4 elements).
fn ipc_body(rows: &[(i64, [f32; 4])]) -> Vec<u8> {
    let element = Arc::new(Field::new("item", DataType::Float32, false));
    let mut lb = ListBuilder::new(Float32Builder::new()).with_field(element.clone());
    let ids: Vec<i64> = rows.iter().map(|(id, _)| *id).collect();
    for (_, emb) in rows {
        lb.values().append_slice(emb);
        lb.append(true);
    }
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("embedding", DataType::List(element), false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(ids)), Arc::new(lb.finish())],
    )
    .expect("batch");
    let mut buf = Vec::new();
    {
        let mut w = StreamWriter::try_new(&mut buf, &schema).expect("writer");
        w.write(&batch).expect("write");
        w.finish().expect("finish");
    }
    buf
}

fn lineage_evt(table: &TableRef) -> LineageEvent {
    LineageEvent {
        run_id: RunId(uuid::Uuid::new_v4()),
        event_type: EventType::Complete,
        event_time: time::OffsetDateTime::now_utc(),
        inputs: vec![],
        outputs: vec![DatasetId::from(table).dataset_ref()],
        payload: serde_json::json!({ "source": "test" }),
    }
}

async fn make_catalog(dsn: String, warehouse: &str) -> SqlCatalog {
    let mut props = HashMap::new();
    props.insert(SQL_CATALOG_PROP_URI.to_string(), dsn);
    props.insert(
        SQL_CATALOG_PROP_WAREHOUSE.to_string(),
        format!("file://{warehouse}"),
    );
    SqlCatalogBuilder::default()
        .with_storage_factory(Arc::new(LocalFsStorageFactory))
        .load("loom", props)
        .await
        .expect("catalog")
}

/// Land `file_rows` as Parquet (inline_byte_limit 0) and `inline_rows` as live
/// inline PG rows (inline_byte_limit usize::MAX), returning the pool + the
/// warehouse TempDir guard (keep alive across the query).
async fn seed(
    fx: &PgFixture,
    db: &str,
    file_rows: &[(i64, [f32; 4])],
    inline_rows: &[(i64, [f32; 4])],
) -> (sqlx::PgPool, tempfile::TempDir) {
    let wh = tempfile::tempdir().expect("wh");
    let pool = fx.pool_for(db).await;
    let catalog = make_catalog(fx.pg_dsn(db), &wh.path().display().to_string()).await;
    let table = TableRef {
        schema: "wh".into(),
        name: "docs".into(),
    };
    if !file_rows.is_empty() {
        land(
            &pool,
            &catalog,
            &table,
            &columns(),
            &ipc_body(file_rows),
            0,
            i64::MAX,
            lineage_evt(&table),
        )
        .await
        .expect("land file rows");
    }
    if !inline_rows.is_empty() {
        land(
            &pool,
            &catalog,
            &table,
            &columns(),
            &ipc_body(inline_rows),
            usize::MAX,
            i64::MAX,
            lineage_evt(&table),
        )
        .await
        .expect("land inline rows");
    }
    (pool, wh)
}

/// Run the projecting SQL and flatten (ids, embeddings) in row order.
async fn read_vectors(pool: sqlx::PgPool) -> (Vec<i64>, Vec<Vec<f32>>) {
    let catalog = IcebergCatalog::new(pool);
    let batches = engine_serving::execute_query(
        &catalog,
        "SELECT \"id\", \"embedding\" FROM \"wh\".\"docs\" ORDER BY \"id\"",
        None,
    )
    .await
    .expect("execute_query projecting a vector column");
    let mut ids = Vec::new();
    let mut embs = Vec::new();
    for b in &batches {
        let id = b
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id is Int64");
        ids.extend((0..id.len()).map(|i| id.value(i)));
        let list = b
            .column(1)
            .as_any()
            .downcast_ref::<ListArray>()
            .expect("embedding is List");
        for row in list.iter() {
            let v = row.expect("non-null embedding");
            let f = v
                .as_any()
                .downcast_ref::<Float32Array>()
                .expect("Float32 child");
            embs.push(f.values().to_vec());
        }
    }
    (ids, embs)
}

const E1: [f32; 4] = [1.0, 0.0, 0.0, 0.5];
const E2: [f32; 4] = [0.0, 1.0, 0.0, 0.25];
const E3: [f32; 4] = [0.0, 0.0, 1.0, 0.125];

/// Files-only: pins the Parquet arm (on-disk child stays Iceberg's "element";
/// the serving schema's child name must keep adapting on read).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sql_projects_vector_from_parquet_files() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let (pool, _wh) = seed(fx, &db, &[(1, E1), (2, E2)], &[]).await;
    let (ids, embs) = read_vectors(pool).await;
    assert_eq!(ids, vec![1, 2]);
    assert_eq!(embs, vec![E1.to_vec(), E2.to_vec()], "file vectors bit-exact");
}

/// Inline-only: THE iss-pg-provider-vector-drift defect — RED today
/// ("inline PG provider: unsupported logical type \"vector(4)\"").
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sql_projects_vector_from_inline_rows() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let (pool, _wh) = seed(fx, &db, &[], &[(1, E1), (2, E2)]).await;
    let (ids, embs) = read_vectors(pool).await;
    assert_eq!(ids, vec![1, 2]);
    assert_eq!(embs, vec![E1.to_vec(), E2.to_vec()], "inline vectors bit-exact");
}

/// Files ∪ inline: both providers must present the SAME vector schema
/// (list child "item") or the union/planning rejects it. RED today.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sql_projects_vector_from_union_of_files_and_inline() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let (pool, _wh) = seed(fx, &db, &[(1, E1), (2, E2)], &[(3, E3)]).await;
    let (ids, embs) = read_vectors(pool).await;
    assert_eq!(ids, vec![1, 2, 3]);
    assert_eq!(
        embs,
        vec![E1.to_vec(), E2.to_vec(), E3.to_vec()],
        "unioned vectors bit-exact"
    );
}
```

Add to `src/services/engine-serving/BUCK` (mirroring the `vector-search` target,
minus `arrow`; `sqlx` stays — the test names `sqlx::PgPool` directly, and a
crate path is only resolvable via a direct dep):

```python
loom_fixture_test(
    name = "inline-vector-sql",
    crate = "inline_vector_sql",
    srcs = ["tests/inline_vector_sql.rs"],
    crate_root = "tests/inline_vector_sql.rs",
    deps = [
        ":engine-serving",
        "//src/control-plane/core:core",
        "//src/control-plane/postgres:postgres",
        "//third-party:arrow-array",
        "//third-party:arrow-ipc",
        "//third-party:arrow-schema",
        "//third-party:iceberg",
        "//third-party:serde_json",
        "//third-party:sqlx",
        "//third-party:tempfile",
        "//third-party:time",
        "//third-party:tokio",
        "//third-party:uuid",
    ],
)
```

- [ ] **Step 2: Run the test — verify the red/green split is exactly the defect**

Run: `buck2 test -j 8 //src/services/engine-serving:inline-vector-sql > /tmp/t3.log 2>&1; grep -E "Tests finished|FAIL|unsupported logical" /tmp/t3.log`
Expected: `sql_projects_vector_from_parquet_files` PASS;
`sql_projects_vector_from_inline_rows` and `…_union_of_files_and_inline` FAIL with
`inline PG provider: unsupported logical type "vector(4)"`.

- [ ] **Step 3: Delete the copies; provider carries `Vec<BaseType>`**

`src/services/engine-serving/src/provider.rs`:

1. Delete `fn pg_rows_to_arrays` entirely (lines 112-202).
2. Imports: drop `Row` from the `sqlx::{…}` import (keep `AssertSqlSafe, PgPool`);
   drop the `arrow::array` builder imports if any remain unused; add
   `use control_plane_core::BaseType;` and
   `use control_plane_postgres::iceberg_inline::column_array;`.
3. Retype the field and constructor:

```rust
    /// loom logical type per column, parallel to `schema.fields()` — resolved ONCE
    /// at construction (an unsupported logical type never constructs a provider),
    /// and decoded by the postgres adapter's shared `column_array`.
    logical_types: Vec<BaseType>,
```

```rust
    pub fn new(
        pool: PgPool,
        relation: String,
        schema: SchemaRef,
        logical_types: Vec<BaseType>,
        base_filter: Option<String>,
    ) -> Self {
```

4. `fetch_batch` takes `proj_logicals: &[BaseType]` and delegates:

```rust
        // One shared decode for ALL PG-row → Arrow reads (the postgres adapter's
        // `column_array`), so this provider can never drift from inline_live_batch
        // again (iss-pg-provider-vector-drift).
        let arrays = proj_logicals
            .iter()
            .enumerate()
            .map(|(i, ty)| {
                column_array(&rows, i, *ty).map_err(|e| ServingError::Engine(e.to_string()))
            })
            .collect::<Result<Vec<_>, _>>()?;
        RecordBatch::try_new(proj_schema, arrays).map_err(|e| ServingError::Engine(e.to_string()))
```

5. In `scan`, the projected pair becomes (`BaseType` is `Copy` — keep the existing
   indexing shape so the lint posture is unchanged):

```rust
        let (proj_schema, proj_logicals): (SchemaRef, Vec<BaseType>) = match projection {
            Some(idx) if idx.is_empty() => (Arc::new(Schema::empty()), Vec::new()),
            Some(idx) => {
                let s = self.schema.project(idx).map_err(|e| {
                    datafusion::error::DataFusionError::ArrowError(Box::new(e), None)
                })?;
                let l = idx.iter().map(|&i| self.logical_types[i]).collect();
                (Arc::new(s), l)
            }
            None => (self.schema.clone(), self.logical_types.clone()),
        };
```

`src/services/engine-serving/src/serving.rs`:

1. Delete `fn base_to_arrow` (lines 214-233).
2. `arrow_schema_from_mirror` maps through core:

```rust
            Ok(Field::new(&c.name, base.arrow_data_type(), c.nullable))
```

3. `build_inline_provider` resolves once, up front:

```rust
    // Resolve every column's logical type ONCE at provider construction — an
    // unsupported type is rejected here, never mid-scan.
    let logical_types = cols
        .iter()
        .map(|c| {
            resolve_logical(&c.ty).ok_or_else(|| {
                EngineServingError::Engine(format!("unknown logical type `{}`", c.ty))
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Some(PgTableProvider::new(
        catalog.pool.clone(),
        inline_table_name(tid),
        schema.clone(),
        logical_types,
        Some(base),
    )))
```

4. Imports: drop `BaseType` and `TimeUnit` (now unused); `Field`, `DataType`,
   `resolve_logical` stay. Leave `time` in `Cargo.toml`/BUCK (still used by the
   fixture tests; not worth lockfile churn — note for `fut-coercion-taxonomy`).

- [ ] **Step 4: Run the new test to green, then the crate's suites**

Run: `buck2 test -j 8 //src/services/engine-serving:inline-vector-sql > /tmp/t3.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t3.log`
Expected: PASS (all three arms). **If the files-only arm regresses** ("cannot cast" /
schema-adapter error on `List` child name), that means DataFusion's default schema
adapter does not relabel list children on this version — STOP and re-review (the
documented fallback is a custom `SchemaAdapterFactory` on `ParquetSource`; do not
improvise it without a human check-in).

Run: `buck2 test -j 8 //src/services/engine-serving: > /tmp/t3b.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t3b.log`
(package-level: pg-scan-sql, execute-query-e2e, execute-query-stream, governed-sql,
vector-search, vector-index-auto-rebuild, action-writer, row-filter-to-expr,
vector-merge, inline-vector-sql). Expected: PASS.

- [ ] **Step 5: Downstream integration suites (query-api + engine serve over the provider)**

Run: `buck2 test -j 8 //src/services/query-api:datafusion-inline-union //src/services/query-api:datafusion-serving //src/services/query-api:iceberg-mirror-provider //src/services/query-api:datafusion-value-map //src/services/query-api:iceberg-pruning-e2e //src/services/query-api:iceberg-schema-evolution-read //src/services/query-api:vector_search_e2e //src/services/query-api:engine-wire-serving-e2e //src/services/engine:vector-search-flight > /tmp/t3c.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t3c.log`
Expected: PASS.

- [ ] **Step 6: Clippy + prek + commit**

Run: `buck2 build '//src/services/engine-serving:engine-serving[clippy.txt]' > /tmp/c3.log 2>&1` — artifact empty.
Run: `buck2 run //tools:prek -- run --all-files > /tmp/p3.log 2>&1; grep -c Failed /tmp/p3.log` — expected `0`.

```bash
git add src/services/engine-serving
git commit -m "fix(engine-serving): serve inline vector columns via the shared PG -> Arrow bridge

Deletes pg_rows_to_arrays (a drifted copy of iceberg_inline::column_array
that lacked the vector arm — a SQL read projecting a vector column of a
table with live inline rows died mid-scan) and base_to_arrow (whose
\"element\" list child diverged from the adapter's \"item\").
PgTableProvider now holds Vec<BaseType> resolved once at construction and
decodes through the adapter's column_array; the serving schema consults
core's arrow_data_type map, collapsing the list-child name to \"item\"
everywhere in memory (Parquet keeps Iceberg's \"element\" on disk; the
new files-arm e2e pins that the schema adapter still reads it).

Fixes iss-pg-provider-vector-drift. Part of road-value-bridge."
```

---

### Task 4: Remaining consumers — flight export + ingest inference consult the map

**Files:**
- Modify: `src/services/query-api/src/flight_export.rs:64-110` (delete `base_to_arrow`)
- Modify: `src/services/datafusion-io/src/infer.rs:30-42` (`logical_arrow_type` delegates)
- Test targets (existing): `//src/services/query-api:export-command`,
  `//src/services/query-api:governed-flight-export-e2e`,
  `//src/services/datafusion-io:logical-arrow`, `//src/services/datafusion-io:infer`

**Interfaces:**
- `export_arrow_schema` keeps its signature (`&[String]` logical types — query-api's
  wire shape is out of scope, see `fut-coercion-taxonomy`); only the per-type mapping
  changes. This flips the *advertised* vector child `element` → `item`, restoring the
  documented lockstep with what the engine now streams (Task 3).
- `logical_arrow_type` keeps its five-name allowlist (the `date → None` pin in
  `logical_arrow.rs` stays green) but sources the `DataType` values from core.

- [ ] **Step 1: flight_export consults core**

In `src/services/query-api/src/flight_export.rs`: delete `fn base_to_arrow` and its
"reproduced here because query-api must not depend on the DataFusion serving crate"
doc comment; in `export_arrow_schema` replace the else-branch:

```rust
            let base = resolve_logical(lt).ok_or_else(|| format!("unknown logical type `{lt}`"))?;
            base.arrow_data_type()
```

Drop the now-unused `BaseType` import (and `Field`/`TimeUnit` only if the compiler
says so — `Field::new` is still used building the schema). Add a one-line comment
where the fn was:

```rust
// BaseType → DataType now lives in core (BaseType::arrow_data_type) — the single
// map shared with the engine's serving schema, so get_flight_info's advertised
// schema and the do_get data schema agree by construction.
```

- [ ] **Step 2: datafusion-io delegates, staying narrow**

Replace `logical_arrow_type` in `src/services/datafusion-io/src/infer.rs`:

```rust
/// loom logical type name -> Arrow `DataType`. The inverse of `arrow_logical_type`,
/// over exactly the five types `infer_columns` round-trips. `None` for an unmapped
/// name (kept deliberately small — YAGNI; widen via `fut-datafusion-type-coverage`),
/// but the `DataType` VALUES come from core's authoritative
/// `BaseType::arrow_data_type` map, so ingest can never disagree with serving on a
/// type it does support.
pub fn logical_arrow_type(ty: &str) -> Option<DataType> {
    match ty {
        "boolean" | "integer" | "long" | "double" | "string" => {
            control_plane_core::resolve_logical(ty)
                .map(control_plane_core::BaseType::arrow_data_type)
        }
        _ => None,
    }
}
```

- [ ] **Step 3: Run the suites**

Run: `buck2 test -j 8 //src/services/query-api:export-command //src/services/query-api:governed-flight-export-e2e //src/services/datafusion-io:logical-arrow //src/services/datafusion-io:infer > /tmp/t4.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t4.log`
Expected: PASS (`export_schema_maps_scalars_and_vector` asserts child type/nullability,
not the name; `logical_arrow_type("date") == None` still holds).

- [ ] **Step 4: Clippy + prek + commit**

Run: `buck2 build '//src/services/query-api:query-api[clippy.txt]' '//src/services/datafusion-io:datafusion-io[clippy.txt]' > /tmp/c4.log 2>&1` — artifacts empty.
Run: `buck2 run //tools:prek -- run --all-files > /tmp/p4.log 2>&1; grep -c Failed /tmp/p4.log` — expected `0`.

```bash
git add src/services/query-api/src/flight_export.rs src/services/datafusion-io/src/infer.rs
git commit -m "refactor(query-api): flight export and ingest infer consult core's arrow map

Deletes flight_export's base_to_arrow copy (the last \"element\" list-child
site) — get_flight_info now advertises vector(N) as List<item: Float32>, in
lockstep with what the engine streams, by consulting the same core map.
datafusion-io's logical_arrow_type keeps its deliberate five-type allowlist
but sources the DataType values from BaseType::arrow_data_type.

Part of road-value-bridge."
```

---

### Task 5: Full sweep + registers

**Files:**
- Modify: `docs/ISSUES.md:26` (`iss-pg-provider-vector-drift` → fixed),
  `docs/ROADMAP.md:282` (`road-value-bridge` → done)

- [ ] **Step 1: Affected-target sweep**

Run the touched crates' full test packages (not a bare whole-tree run; `-j 8` for the
fixture slots):

`buck2 test -j 8 //src/control-plane/core: //src/control-plane/postgres: //src/services/engine-serving: //src/services/datafusion-io: //src/services/query-api: //src/services/engine: //src/services/worker: > /tmp/t5.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t5.log`

Expected: PASS. Verify `git status src/control-plane/postgres/.sqlx` is clean and
`git diff origin/main -- third-party/BUCK` is empty.

- [ ] **Step 2: Close the register items**

In `docs/ISSUES.md`, flip the entry to
`- [x] **Inline PG provider drops vector columns (drifted conversion copy)** `
with `status:fixed` and `pr:#N`; in `docs/ROADMAP.md`, flip `road-value-bridge` to
`- [x]` / `status:done` / `pr:#N` — substitute the real PR number when the PR exists
(open the PR first, then push this commit to the same branch; or amend at PR time).
Validate: `bash tools/docs.sh validate`.

- [ ] **Step 3: prek + commit**

Run: `buck2 run //tools:prek -- run --all-files > /tmp/p5.log 2>&1; grep -c Failed /tmp/p5.log` — expected `0`.

```bash
git add docs/ISSUES.md docs/ROADMAP.md
git commit -m "docs(registers): close road-value-bridge and iss-pg-provider-vector-drift"
```

Then finish the branch per `superpowers:finishing-a-development-branch` (push +
open PR against `main`; CI green via the commit-status endpoint + BuildBuddy MCP,
not `gh pr checks`).
