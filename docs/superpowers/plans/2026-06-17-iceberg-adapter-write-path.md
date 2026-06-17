# Iceberg Adapter — Write Path (Slice 2) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Turn the slice-1 test-only synthetic-file writer into a real Iceberg write path — real Parquet bytes via the `iceberg` writer chain, committed atomically (Iceberg pointer CAS + loom mirror projection in one Postgres transaction), with concurrency-safe snapshot ids.

**Architecture:** The `iceberg` crate's only public commit path funnels through `Catalog::update_table`, which loom owns (vendored at `src/control-plane/postgres/src/iceberg_sql_catalog/`). That method becomes the single atomic chokepoint: it opens one `Transaction<Postgres>`, runs the pointer compare-and-swap, then projects the loom `iceberg_mirror.*` rows **derived from the canonical staged Iceberg metadata** — all in one commit. A new `iceberg_writer` library module drives Arrow batches through the writer chain to real Parquet and issues the `fast_append`. The slice-1 `IcebergCatalog` read path is unchanged and serves as the round-trip oracle.

**Tech Stack:** Rust, buck2, `iceberg` 0.9.1 (vendored SQL catalog), arrow-array/arrow-schema/parquet **57.3.1** (writer chain), sqlx 0.9 compile-time macros, Postgres, reindeer (third-party importer).

**Spec:** `docs/superpowers/specs/2026-06-17-iceberg-adapter-write-path-design.md`

---

## Background the implementer needs

- **Tests are `rust_test` integration targets**, never inline `#[cfg(test)]` (a prek hook enforces this). Fixture-backed tests use the `loom_fixture_test` macro (`src/control-plane/postgres/defs.bzl`) so the test command runs locally (it boots `initdb`/`postgres`, which refuse to run as root on remote execution).
- **Don't pipe `buck2 test` through `tail`/`head`** — it stalls. Redirect to a file and grep: `buck2 test //src/control-plane/postgres/... > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`.
- **After changing SQL that compile-time `sqlx::query!` macros see**, regenerate the committed cache: `./tools/sqlx-prepare.sh`, then commit `src/control-plane/postgres/.sqlx/`. The `sqlx-cache-check` test fails otherwise.
- **After changing any `Cargo.toml` dependency**, refresh the lock and regenerate `third-party/BUCK`: `cargo generate-lockfile` (or `buck2 run //tools:reindeer -- update`) then `./tools/buckify.sh`. The `reindeer-check` hook fails if `third-party/BUCK` drifts from the manifests.
- **Clippy across all first-party Rust:** `./tools/clippy-all.sh`. **All hooks:** `buck2 run //tools:prek -- run --all-files`.

### What slice 1 left in place (read before editing)

- `src/control-plane/postgres/src/iceberg_sql_catalog/catalog.rs` — vendored `SqlCatalog`. Struct fields: `name: String`, `connection: PgPool`, `fileio: FileIO`. Key internals:
  - `execute(&self, query, args, transaction: Option<&mut Transaction<'_, Postgres>>)` (≈line 303) — **already** threads an optional Postgres transaction; with `None` it opens+commits its own.
  - `update_table(&self, commit: TableCommit)` (≈line 902) — loads current table, applies the commit to a `staged_table`, writes staged metadata JSON via `staged_table.metadata().write_to(file_io, loc)`, then CAS-updates the pointer with `self.execute(…, None)`. A `rows_affected() == 0` is a retryable `CatalogCommitConflicts`.
  - `drop_table(&self, identifier)` (≈line 688) — deletes the pointer row.
  - `create_table` (≈line 761) — writes initial metadata + inserts the pointer row.
- `src/control-plane/postgres/src/iceberg_mirror.rs` — projection helpers operating on `&mut PgConnection`: `next_snapshot`, `ensure_table`, `project_columns`, `project_files`, `mark_dropped`; structs `ProjectedColumn { order, name, iceberg_type, nullable }`, `ProjectedFile { path, file_format, record_count, file_size_bytes }`. `next_snapshot` currently uses `max(snapshot_id)+1` with a `FIXME(slice-2)`.
- `src/control-plane/postgres/src/iceberg_catalog.rs` — `IcebergCatalog { pool }` impl `core::Catalog`. Reads key on `table_namespace = TableRef.schema` and `table_name = TableRef.name`, with MVCC predicate `begin_snapshot <= $at and (end_snapshot is null or end_snapshot > $at)`.
- `src/control-plane/postgres/src/fixture.rs` — `IcebergWriter` seeder (≈line 486): `new(pool, pg_dsn)`, `catalog()` builds the vendored `SqlCatalog` over a `file://` tempdir warehouse, `seed(ns, name, columns, batches)` creates the table + projects synthetic files, `drop_table(ns, name)`. Also `PgFixture::pg_dsn(db)` and `PgControlPlane::pool()`.
- `src/control-plane/postgres/tests/iceberg_catalog.rs` — `IcebergSeeder` impl `CatalogSeed`; `iceberg_passes_catalog_contract` + `iceberg_passes_catalog_delete_contract`.

### The writer chain (verified against the pinned iceberg 0.9.1 source)

```rust
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::file_writer::location_generator::{DefaultFileNameGenerator, DefaultLocationGenerator};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use parquet::file::properties::WriterProperties;

let location_generator = DefaultLocationGenerator::new(table.metadata().clone())?;
let file_name_generator = DefaultFileNameGenerator::new(
    "loom".to_string(), None, iceberg::spec::DataFileFormat::Parquet);
let parquet_builder = ParquetWriterBuilder::new(
    WriterProperties::default(), table.metadata().current_schema().clone()); // iceberg SchemaRef, not arrow
let rolling = RollingFileWriterBuilder::new_with_default_file_size(
    parquet_builder, table.file_io().clone(), location_generator, file_name_generator);
let mut writer = DataFileWriterBuilder::new(rolling).build(None).await?;
writer.write(record_batch).await?;          // arrow_array::RecordBatch
let data_files = writer.close().await?;     // Vec<iceberg::spec::DataFile>
```

Commit (the only public path; funnels to `catalog.update_table`):

```rust
use iceberg::transaction::{ApplyTransactionAction, Transaction};
let tx = Transaction::new(&table);
let action = tx.fast_append().add_data_files(data_files);  // fast_append(&self) -> owned action
let tx = action.apply(tx)?;                                // ApplyTransactionAction::apply(self, tx)
let committed: iceberg::table::Table = tx.commit(&catalog).await?;
```

`iceberg::arrow::schema_to_arrow_schema(&iceberg::spec::Schema) -> iceberg::Result<arrow_schema::Schema>` builds the arrow schema for `RecordBatch::try_new`.

---

## File Structure

| File | Responsibility | Task |
|------|----------------|------|
| `third-party/fixups/{parquet,arrow-array,arrow-schema}/fixups.toml` | Make v57 targets PUBLIC (or buckify.sh fallback) | 0 |
| `tools/buckify.sh` | Fallback only: named-allowlist v57 visibility pass | 0 |
| `src/services/datafusion-io/BUCK` | Immunize bare `//third-party:parquet` → `:parquet-58` if reindeer flips the alias | 0 |
| `src/control-plane/postgres/Cargo.toml` | Add `arrow-array`/`arrow-schema`/`parquet` `= "57"` | 0 |
| `src/control-plane/postgres/BUCK` | v57 deps on lib + new writer test targets | 0,2,3,4 |
| `src/control-plane/postgres/migrations/0013_iceberg_snapshot_seq.sql` | `create sequence iceberg_mirror.snapshot_seq` | 1 |
| `src/control-plane/postgres/src/iceberg_mirror.rs` | `next_snapshot` via `nextval`; manifest→`ProjectedFile` derivation; "columns-absent" guard | 1,3 |
| `src/control-plane/postgres/src/iceberg_writer.rs` | New: real Parquet writer + `fast_append` commit | 2 |
| `src/control-plane/postgres/src/iceberg_sql_catalog/catalog.rs` | `update_table`+`drop_table`: one tx threading CAS + mirror projection | 3 |
| `src/control-plane/postgres/src/lib.rs` | `pub mod iceberg_writer;` | 2 |
| `src/control-plane/postgres/src/fixture.rs` | `IcebergWriter` drives the real writer | 4 |
| `src/control-plane/postgres/.sqlx/` | Regenerated after SQL changes | 1,3 |

---

## Task 0: third-party v57 visibility

**Why first:** nothing in the writer compiles until the v57 arrow/parquet targets are reachable from `//src/control-plane/postgres`. Slice 1 hit this wall. Key facts established during planning:
- reindeer's **bare** unversioned alias (`//third-party:parquet`) points to the version a *workspace member directly depends on* (`parquet`→`:parquet-58` because `src/services/ingest` deps `parquet = "58"`; analogously `thiserror`→`:thiserror-1`). Adding `parquet = "57"` as a second direct dep is what "collided" in slice 1.
- `arrow-array` and `arrow-schema` have **no** bare alias (nothing deps them directly), so a v57 direct dep on them is collision-free and creates fresh `//third-party:arrow-array` / `:arrow-schema` aliases pointing at 57.
- The only first-party consumer of the bare `//third-party:parquet` alias is `src/services/datafusion-io/BUCK`. If reindeer flips that alias off 58, only datafusion-io is at risk — immunize it by pinning to the versioned `:parquet-58`.

> **AS-BUILT (commit a26533c):** the plan's reindeer-fixup-first / bare-alias / datafusion-io-contingency
> path was superseded after reading reindeer's source. Findings: (1) the `visibility` fixup only tunes the
> bare *alias*, never the versioned lib, so it can't expose `:parquet-57`; (2) a Cargo dep *rename* (the
> native disambiguator) is honored **only from a root package's deps**, and loom is a **virtual workspace**
> (`resolve.root == None`), so member renames are ignored — verified empirically. So `buckify.sh` gained two
> deterministic passes: **widen** the `:*-57` targets to PUBLIC, and **dedupe** each bare alias to its
> highest version (parquet→58), leaving `datafusion-io`/`ingest` untouched (no contingency edit needed).
> postgres deps the explicit `//third-party:{parquet-57,arrow-array-57,arrow-schema-57}`. No fixups created.
> The lock already had arrow/parquet 57 (transitive via iceberg) — edit Cargo.toml + run buckify only; do
> NOT `cargo generate-lockfile` (it re-resolves the graph and drops unrelated crates).

**Files (as-built):**
- Modify: `src/control-plane/postgres/Cargo.toml` (add `arrow-array`/`arrow-schema`/`parquet` `= "57"`)
- Modify: `tools/buckify.sh` (widen `:*-57` to PUBLIC + dedupe bare alias to highest version)
- Modify: `src/control-plane/postgres/BUCK` (dep the versioned `:*-57` targets)
- Modify: `src/control-plane/postgres/src/lib.rs` (temp visibility probe; removed in Task 2)

- [ ] **Step 1: Add the v57 deps to the postgres manifest**

In `src/control-plane/postgres/Cargo.toml`, under `[dependencies]`, after `iceberg = "0.9"`:

```toml
arrow-array = "57"
arrow-schema = "57"
parquet = { version = "57", default-features = false, features = ["arrow"] }
```

- [ ] **Step 2: Refresh the lock and regenerate third-party rules**

```bash
cd /home/jackm/repos/loom
cargo generate-lockfile
./tools/buckify.sh
```

Expected: `third-party/BUCK` regenerates. `arrow-array 57.3.1` / `arrow-schema 57.3.1` / `parquet 57.3.1` were already in the lock (iceberg pulled them transitively), so no version churn.

- [ ] **Step 3: Verify the bare aliases did not regress**

```bash
cd /home/jackm/repos/loom
python3 - <<'PY'
import re
s = open('third-party/BUCK').read()
for name in ('parquet', 'arrow'):
    m = re.search(r'name = "'+name+r'",\s*\n\s*actual = "(:[^"]+)"', s)
    print(name, '->', m.group(1) if m else 'NO BARE ALIAS')
PY
```

Expected: `parquet -> :parquet-58` and `arrow -> :arrow-58` (unchanged — we added `arrow-array`/`arrow-schema`, not the `arrow` meta-crate; `parquet`'s bare alias stays 58 because ingest still directly deps 58). **If `parquet` flipped off `:parquet-58`**, immunize the lone consumer: in `src/services/datafusion-io/BUCK` replace every `"//third-party:parquet"` with `"//third-party:parquet-58"`, and re-run the check.

- [ ] **Step 4: Make the v57 targets PUBLIC — try the reindeer fixup first**

Create three fixups, each containing exactly:

```toml
# third-party/fixups/parquet/fixups.toml  (and arrow-array/, arrow-schema/)
visibility = ["PUBLIC"]
```

Regenerate and inspect:

```bash
cd /home/jackm/repos/loom
./tools/buckify.sh 2>&1 | tee /tmp/buckify.log
grep -nE 'name = "(parquet|arrow-array|arrow-schema)-57"' -A30 third-party/BUCK | grep -E 'name = "(parquet|arrow-array|arrow-schema)-57"|visibility'
```

Expected (success): the `parquet-57`, `arrow-array-57`, `arrow-schema-57` `cargo.rust_library` rules now show `visibility = ["PUBLIC"]`. **If reindeer errors** (e.g. "visibility only settable on public packages") or leaves them `visibility = []`, delete the three fixup files and go to Step 5 (the fallback). Otherwise skip Step 5.

- [ ] **Step 5 (fallback only): add a visibility pass to buckify.sh**

Append a third post-processing fix inside the `PYFIX` python heredoc in `tools/buckify.sh`, immediately before the `with open(path, "w")` write-back:

```python
# (3) Widen visibility for the iceberg writer-chain crates. iceberg 0.9.1 pins
#     arrow/parquet 57, but only their transitive (non-public) versioned targets
#     exist, so reindeer marks them visibility=[] (package-private). The first-
#     party iceberg writer (src/control-plane/postgres) needs to name them
#     directly. Promote exactly the crates it uses to PUBLIC.
PUBLIC_57 = ["parquet-57", "arrow-array-57", "arrow-schema-57"]
for name in PUBLIC_57:
    lib = re.compile(
        r'(cargo\.rust_library\(\s*\n\s*name = "' + re.escape(name) + r'",.*?\n)(\s*)visibility = \[\]',
        re.DOTALL)
    content = lib.sub(lambda m: m.group(1) + m.group(2) + 'visibility = ["PUBLIC"]', content)
```

Re-run and verify:

```bash
cd /home/jackm/repos/loom
./tools/buckify.sh
grep -nE 'name = "(parquet|arrow-array|arrow-schema)-57"' -A30 third-party/BUCK | grep -E 'name = "(parquet|arrow-array|arrow-schema)-57"|visibility'
```

Expected: the three `-57` libraries show `visibility = ["PUBLIC"]`.

- [ ] **Step 6: Wire the v57 deps into the postgres library and prove visibility**

In `src/control-plane/postgres/BUCK`, add to the `:postgres` `rust_library` `deps` (the list containing `//third-party:iceberg`):

```python
        "//third-party:parquet-57",
        "//third-party:arrow-array-57",
        "//third-party:arrow-schema-57",
```

Add a temporary compile reference so the build actually exercises the visibility (the writer module arrives in Task 2). At the bottom of `src/control-plane/postgres/src/lib.rs`:

```rust
// TEMP (removed in Task 2 when iceberg_writer lands): prove the v57 third-party
// targets are visible to this crate.
#[allow(unused_imports)]
use arrow_array::RecordBatch as _Slice2VisibilityProbeBatch;
#[allow(unused_imports)]
use parquet::file::properties::WriterProperties as _Slice2VisibilityProbeProps;
```

- [ ] **Step 7: Build to confirm**

```bash
cd /home/jackm/repos/loom
buck2 build //src/control-plane/postgres:postgres 2>&1 | tail -5
```

Expected: builds clean. (Before the visibility fix this fails with a buck2 "not visible to //src/control-plane/postgres" error on `//third-party:parquet-57`.)

- [ ] **Step 8: Commit**

```bash
cd /home/jackm/repos/loom
git add src/control-plane/postgres/Cargo.toml Cargo.lock third-party/ tools/buckify.sh src/control-plane/postgres/BUCK src/control-plane/postgres/src/lib.rs src/services/datafusion-io/BUCK 2>/dev/null
git commit -m "build(iceberg): expose arrow/parquet 57 to the postgres crate for the writer chain"
```

---

## Task 1: concurrency-safe snapshot ids via a Postgres sequence

**Files:**
- Create: `src/control-plane/postgres/migrations/0013_iceberg_snapshot_seq.sql`
- Modify: `src/control-plane/postgres/src/iceberg_mirror.rs` (`next_snapshot`)
- Create: `src/control-plane/postgres/tests/iceberg_snapshot_seq.rs`
- Modify: `src/control-plane/postgres/BUCK` (new test target)
- Regenerate: `src/control-plane/postgres/.sqlx/`

- [ ] **Step 1: Write the failing test**

Create `src/control-plane/postgres/tests/iceberg_snapshot_seq.rs`:

```rust
//! `next_snapshot` must hand out distinct, monotonically increasing ids under
//! concurrency — the property the retired `max(snapshot_id)+1` could not give
//! (two callers read the same max and collide on the PK).
use std::collections::HashSet;

use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_mirror::next_snapshot;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn next_snapshot_is_concurrency_safe() {
    let fx = PgFixture::start();
    let cp = fx.fresh_control_plane().await;
    let pool = cp.pool().clone();

    const N: usize = 32;
    let mut handles = Vec::new();
    for _ in 0..N {
        let pool = pool.clone();
        handles.push(tokio::spawn(async move {
            let mut conn = pool.acquire().await.expect("acquire");
            next_snapshot(&mut conn, None).await.expect("next_snapshot").0
        }));
    }
    let mut ids = Vec::new();
    for h in handles {
        ids.push(h.await.expect("join"));
    }
    let unique: HashSet<i64> = ids.iter().copied().collect();
    assert_eq!(unique.len(), N, "snapshot ids must be distinct: {ids:?}");
    assert!(ids.iter().all(|&id| id >= 1), "ids start at 1: {ids:?}");
}
```

Add the test target to `src/control-plane/postgres/BUCK` (mirror the existing `iceberg_catalog` `loom_fixture_test`):

```python
loom_fixture_test(
    name = "iceberg-snapshot-seq",
    srcs = ["tests/iceberg_snapshot_seq.rs"],
    crate = "iceberg_snapshot_seq",
    crate_root = "tests/iceberg_snapshot_seq.rs",
    deps = [":postgres", "//third-party:tokio"],
)
```

- [ ] **Step 2: Run it to confirm it fails**

```bash
cd /home/jackm/repos/loom
buck2 test //src/control-plane/postgres:iceberg-snapshot-seq > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|error" /tmp/t.log | head
```

Expected: FAIL — either a build error (no migration/sequence yet) or, with the current `max+1`, duplicate ids under contention. (If it happens to pass by luck, the sequence change below makes it deterministic.)

- [ ] **Step 3: Add the sequence migration**

Create `src/control-plane/postgres/migrations/0013_iceberg_snapshot_seq.sql`:

```sql
-- Catalog-global monotonic snapshot ids, concurrency-safe. Replaces the
-- max(snapshot_id)+1 allocation in next_snapshot (two writers could read the
-- same max and collide on the snapshot PK). nextval is atomic and never reuses
-- a value; gaps (from rolled-back commits) are acceptable — only monotonicity
-- and uniqueness matter to the mirror's MVCC ordering.
create sequence iceberg_mirror.snapshot_seq as bigint start with 1 increment by 1;
```

- [ ] **Step 4: Switch `next_snapshot` to `nextval`**

In `src/control-plane/postgres/src/iceberg_mirror.rs`, replace the body of `next_snapshot` (the `FIXME(slice-2)` comment and the `max(...)+1` query) with:

```rust
pub async fn next_snapshot(
    conn: &mut PgConnection,
    iceberg_snapshot_id: Option<i64>,
) -> Result<SnapshotId> {
    let id = sqlx::query_scalar!("select nextval('iceberg_mirror.snapshot_seq') as \"next!\"")
        .fetch_one(&mut *conn)
        .await
        .map_err(backend)?;
    sqlx::query!(
        "insert into iceberg_mirror.snapshot (snapshot_id, iceberg_snapshot_id) values ($1, $2)",
        id,
        iceberg_snapshot_id,
    )
    .execute(&mut *conn)
    .await
    .map_err(backend)?;
    Ok(SnapshotId(id))
}
```

- [ ] **Step 5: Regenerate the sqlx cache**

```bash
cd /home/jackm/repos/loom
./tools/sqlx-prepare.sh
```

Expected: a new/changed `src/control-plane/postgres/.sqlx/query-*.json` for the `nextval` query.

- [ ] **Step 6: Run the test to confirm it passes**

```bash
cd /home/jackm/repos/loom
buck2 test //src/control-plane/postgres:iceberg-snapshot-seq > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log
```

Expected: PASS (32 distinct ids).

- [ ] **Step 7: Commit**

```bash
cd /home/jackm/repos/loom
git add src/control-plane/postgres/migrations/0013_iceberg_snapshot_seq.sql src/control-plane/postgres/src/iceberg_mirror.rs src/control-plane/postgres/tests/iceberg_snapshot_seq.rs src/control-plane/postgres/BUCK src/control-plane/postgres/.sqlx
git commit -m "feat(iceberg): concurrency-safe snapshot ids via iceberg_mirror.snapshot_seq"
```

---

## Task 2: the real Parquet writer (`iceberg_writer`)

Builds the writer-chain module that produces real Parquet and issues the `fast_append`. At this point `update_table` is still the unmodified vendored CAS-only commit, so the mirror is **not** populated by this path yet — this task proves only that real, valid Parquet is written and the Iceberg commit succeeds. The mirror round-trip arrives in Task 3.

**Files:**
- Create: `src/control-plane/postgres/src/iceberg_writer.rs`
- Modify: `src/control-plane/postgres/src/lib.rs` (declare module; remove the Task-0 probe)
- Create: `src/control-plane/postgres/tests/iceberg_writer.rs`
- Modify: `src/control-plane/postgres/BUCK` (new test target)

- [ ] **Step 1: Write the failing test**

Create `src/control-plane/postgres/tests/iceberg_writer.rs`:

```rust
//! The writer produces real, valid Parquet and commits it as a fast_append.
//! Reads the bytes back with the parquet reader to prove they are real Parquet
//! (not just catalog metadata).
use std::collections::HashMap;
use std::fs::File;
use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_writer::append_batches;
use control_plane_postgres::iceberg_sql_catalog::{
    SqlCatalogBuilder, SQL_CATALOG_PROP_URI, SQL_CATALOG_PROP_WAREHOUSE,
};
use iceberg::io::LocalFsStorageFactory;
use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
use iceberg::writer::file_writer::location_generator::DefaultLocationGenerator;
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation, TableIdent};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn writes_real_parquet_and_commits() {
    let fx = PgFixture::start();
    let (_cp, db) = fx.fresh_db().await;
    let warehouse = tempfile::tempdir().expect("warehouse");

    // Build the vendored catalog over the fixture DB + a file:// warehouse.
    let mut props = HashMap::new();
    props.insert(SQL_CATALOG_PROP_URI.to_string(), fx.pg_dsn(&db));
    props.insert(
        SQL_CATALOG_PROP_WAREHOUSE.to_string(),
        format!("file://{}", warehouse.path().display()),
    );
    let catalog = SqlCatalogBuilder::default()
        .with_storage_factory(Arc::new(LocalFsStorageFactory))
        .load("loom", props)
        .await
        .expect("catalog");

    let ns = NamespaceIdent::new("wh".to_string());
    catalog.create_namespace(&ns, HashMap::new()).await.expect("ns");
    let schema = Schema::builder()
        .with_fields([
            Arc::new(NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long))),
            Arc::new(NestedField::optional(2, "name", Type::Primitive(PrimitiveType::String))),
        ])
        .build()
        .expect("schema");
    let creation = TableCreation::builder()
        .name("t".to_string())
        .location(format!("file://{}/wh/t", warehouse.path().display()))
        .schema(schema)
        .build();
    catalog.create_table(&ns, creation).await.expect("create");

    let table = catalog
        .load_table(&TableIdent::new(ns.clone(), "t".to_string()))
        .await
        .expect("load");

    let batch = RecordBatch::try_new(
        Arc::new(iceberg::arrow::schema_to_arrow_schema(table.metadata().current_schema()).unwrap()),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["a", "b", "c"])),
        ],
    )
    .expect("batch");

    let data_files = append_batches(&catalog, &table, vec![batch]).await.expect("append");

    assert_eq!(data_files.len(), 1, "one rolling file for a small batch");
    let df = &data_files[0];
    assert_eq!(df.record_count, 3);
    assert!(df.file_size_bytes > 0);

    // Resolve the on-disk path (DefaultLocationGenerator writes under the table location)
    // and read it back with the parquet reader to prove valid Parquet.
    let _loc = DefaultLocationGenerator::new(table.metadata().clone()).unwrap();
    let path = df.path.strip_prefix("file://").unwrap_or(&df.path);
    let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(path).expect("open parquet"))
        .expect("parquet reader")
        .build()
        .expect("build reader");
    let rows: usize = reader.map(|b| b.expect("batch").num_rows()).sum();
    assert_eq!(rows, 3, "parquet bytes hold the 3 written rows");
}
```

Add to `src/control-plane/postgres/BUCK`:

```python
loom_fixture_test(
    name = "iceberg-writer",
    srcs = ["tests/iceberg_writer.rs"],
    crate = "iceberg_writer",
    crate_root = "tests/iceberg_writer.rs",
    deps = [
        ":postgres",
        "//third-party:iceberg",
        "//third-party:arrow-array-57",
        "//third-party:parquet-57",
        "//third-party:tempfile",
        "//third-party:tokio",
    ],
)
```

- [ ] **Step 2: Run it to confirm it fails**

```bash
cd /home/jackm/repos/loom
buck2 test //src/control-plane/postgres:iceberg-writer > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|error\[|cannot find" /tmp/t.log | head
```

Expected: build failure — `append_batches` / re-exports don't exist yet.

- [ ] **Step 3: Implement the writer module**

Create `src/control-plane/postgres/src/iceberg_writer.rs`:

```rust
//! Real Iceberg write path: drives Arrow record batches through the `iceberg`
//! writer chain to real Parquet, then commits them as a `fast_append`. The
//! commit funnels through the vendored catalog's `update_table`, which is where
//! loom projects the `iceberg_mirror.*` rows atomically (see `iceberg_sql_catalog`).
//! This module is pure write — it holds no mirror logic.

use arrow_array::RecordBatch;
use iceberg::spec::DataFile;
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use iceberg::{Catalog, Result};
use parquet::file::properties::WriterProperties;

/// A neutral summary of one committed Parquet data file, returned to callers
/// (tests, the seeder) that want to assert on the write without depending on
/// the `iceberg::spec::DataFile` shape.
pub struct WrittenFile {
    pub path: String,
    pub record_count: i64,
    pub file_size_bytes: i64,
}

/// Write `batches` to real Parquet under `table`'s location and commit them as a
/// single `fast_append`. Returns one `WrittenFile` per produced data file. The
/// caller must have created `table` in `catalog` already. The mirror projection
/// happens inside `catalog.update_table` during `commit` — not here.
pub async fn append_batches(
    catalog: &dyn Catalog,
    table: &Table,
    batches: Vec<RecordBatch>,
) -> Result<Vec<WrittenFile>> {
    let data_files = write_parquet(table, batches).await?;
    let summaries: Vec<WrittenFile> = data_files
        .iter()
        .map(|df| WrittenFile {
            path: df.file_path().to_string(),
            record_count: df.record_count() as i64,
            file_size_bytes: df.file_size_in_bytes() as i64,
        })
        .collect();

    let tx = Transaction::new(table);
    let action = tx.fast_append().add_data_files(data_files);
    let tx = action.apply(tx)?;
    tx.commit(catalog).await?;
    Ok(summaries)
}

async fn write_parquet(table: &Table, batches: Vec<RecordBatch>) -> Result<Vec<DataFile>> {
    let location_generator = DefaultLocationGenerator::new(table.metadata().clone())?;
    let file_name_generator =
        DefaultFileNameGenerator::new("loom".to_string(), None, iceberg::spec::DataFileFormat::Parquet);
    let parquet_builder = ParquetWriterBuilder::new(
        WriterProperties::default(),
        table.metadata().current_schema().clone(),
    );
    let rolling = RollingFileWriterBuilder::new_with_default_file_size(
        parquet_builder,
        table.file_io().clone(),
        location_generator,
        file_name_generator,
    );
    let mut writer = DataFileWriterBuilder::new(rolling).build(None).await?;
    for batch in batches {
        writer.write(batch).await?;
    }
    writer.close().await
}
```

In `src/control-plane/postgres/src/lib.rs`: remove the two TEMP `#[allow(unused_imports)] use …` probe lines from Task 0, and add the module declaration next to the other iceberg modules:

```rust
pub mod iceberg_writer;
```

The test references `control_plane_postgres::iceberg_sql_catalog::{SqlCatalogBuilder, SQL_CATALOG_PROP_URI, SQL_CATALOG_PROP_WAREHOUSE}`. Confirm these are re-exported from `iceberg_sql_catalog/mod.rs`; if `SQL_CATALOG_PROP_URI` / `SQL_CATALOG_PROP_WAREHOUSE` are not already `pub use`-d there, add them (they are defined in the vendored `catalog.rs`):

```rust
// src/control-plane/postgres/src/iceberg_sql_catalog/mod.rs
pub use catalog::{SqlCatalog, SqlCatalogBuilder, SQL_CATALOG_PROP_URI, SQL_CATALOG_PROP_WAREHOUSE};
```

- [ ] **Step 4: Run the test to confirm it passes**

```bash
cd /home/jackm/repos/loom
buck2 test //src/control-plane/postgres:iceberg-writer > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log
```

Expected: PASS — one data file, 3 rows, readable by the parquet reader.

- [ ] **Step 5: Clippy + commit**

```bash
cd /home/jackm/repos/loom
buck2 build '//src/control-plane/postgres:postgres[clippy.txt]' 2>&1 | tail -3
git add src/control-plane/postgres/src/iceberg_writer.rs src/control-plane/postgres/src/lib.rs src/control-plane/postgres/src/iceberg_sql_catalog/mod.rs src/control-plane/postgres/tests/iceberg_writer.rs src/control-plane/postgres/BUCK
git commit -m "feat(iceberg): iceberg_writer — real Parquet via the writer chain + fast_append"
```

---

## Task 3: atomic mirror projection inside `update_table` (and `drop_table`)

Make the vendored commit chokepoint project the loom mirror from the canonical staged Iceberg metadata, in the **same** Postgres transaction as the pointer CAS. After this task, a `fast_append` via `iceberg_writer` populates the mirror, so the slice-1 `IcebergCatalog` read path can round-trip it.

**Files:**
- Modify: `src/control-plane/postgres/src/iceberg_sql_catalog/catalog.rs` (`update_table`, `drop_table`)
- Modify: `src/control-plane/postgres/src/iceberg_mirror.rs` (manifest→`ProjectedFile` derivation, columns-absent guard)
- Create: `src/control-plane/postgres/tests/iceberg_write_roundtrip.rs`
- Modify: `src/control-plane/postgres/BUCK` (new test target)
- Regenerate: `src/control-plane/postgres/.sqlx/`

- [ ] **Step 1: Write the failing round-trip + atomicity test**

Create `src/control-plane/postgres/tests/iceberg_write_roundtrip.rs`:

```rust
//! Writing via iceberg_writer populates the loom mirror atomically with the
//! Iceberg pointer, so the slice-1 IcebergCatalog read path round-trips it; and
//! concurrent appends leave the mirror consistent (one snapshot row per commit,
//! every snapshot carrying its files — no orphans from rolled-back CAS attempts).
use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use control_plane_core::catalog::{Catalog, PageReq, SnapshotId, TableRef};
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use control_plane_postgres::iceberg_sql_catalog::{
    SqlCatalog, SqlCatalogBuilder, SQL_CATALOG_PROP_URI, SQL_CATALOG_PROP_WAREHOUSE,
};
use control_plane_postgres::iceberg_writer::append_batches;
use iceberg::io::LocalFsStorageFactory;
use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
use iceberg::{Catalog as _, CatalogBuilder, NamespaceIdent, TableCreation, TableIdent};
use sqlx::PgPool;

async fn make_catalog(dsn: String, warehouse: &str) -> SqlCatalog {
    let mut props = HashMap::new();
    props.insert(SQL_CATALOG_PROP_URI.to_string(), dsn);
    props.insert(SQL_CATALOG_PROP_WAREHOUSE.to_string(), format!("file://{warehouse}"));
    SqlCatalogBuilder::default()
        .with_storage_factory(Arc::new(LocalFsStorageFactory))
        .load("loom", props)
        .await
        .expect("catalog")
}

async fn create_t(catalog: &SqlCatalog, warehouse: &str) {
    let ns = NamespaceIdent::new("wh".to_string());
    catalog.create_namespace(&ns, HashMap::new()).await.expect("ns");
    let schema = Schema::builder()
        .with_fields([
            Arc::new(NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long))),
            Arc::new(NestedField::optional(2, "name", Type::Primitive(PrimitiveType::String))),
        ])
        .build()
        .expect("schema");
    catalog
        .create_table(
            &ns,
            TableCreation::builder()
                .name("t".to_string())
                .location(format!("file://{warehouse}/wh/t"))
                .schema(schema)
                .build(),
        )
        .await
        .expect("create");
}

fn batch(catalog_schema: &iceberg::spec::Schema, ids: Vec<i64>) -> RecordBatch {
    let names: Vec<String> = ids.iter().map(|i| format!("row{i}")).collect();
    RecordBatch::try_new(
        Arc::new(iceberg::arrow::schema_to_arrow_schema(catalog_schema).unwrap()),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(names.iter().map(|s| s.as_str()).collect::<Vec<_>>())),
        ],
    )
    .expect("batch")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn append_round_trips_through_the_mirror() {
    let fx = PgFixture::start();
    let (_cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("wh");
    let whs = wh.path().display().to_string();
    let catalog = make_catalog(fx.pg_dsn(&db), &whs).await;
    create_t(&catalog, &whs).await;
    let table = catalog
        .load_table(&TableIdent::new(NamespaceIdent::new("wh".into()), "t".into()))
        .await
        .expect("load");
    let cs = table.metadata().current_schema().clone();

    append_batches(&catalog, &table, vec![batch(&cs, vec![1, 2, 3])]).await.expect("append");

    let pool: PgPool = fx.pool_for(&db).await;
    let read = IcebergCatalog::new(pool);
    let tref = TableRef { schema: "wh".into(), name: "t".into() };
    let snap = read.current_snapshot(&tref).await.expect("current_snapshot");
    let files = read.files(&tref, snap.id, PageReq::first(100)).await.expect("files");
    assert_eq!(files.items.len(), 1, "one data file in the mirror");
    assert_eq!(files.items[0].record_count, 3);
    let schema = read.schema(&tref, snap.id).await.expect("schema");
    assert_eq!(schema.columns.len(), 2, "id + name projected");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_appends_keep_the_mirror_consistent() {
    let fx = PgFixture::start();
    let (_cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("wh");
    let whs = wh.path().display().to_string();
    let setup = make_catalog(fx.pg_dsn(&db), &whs).await;
    create_t(&setup, &whs).await;
    let cs = setup
        .load_table(&TableIdent::new(NamespaceIdent::new("wh".into()), "t".into()))
        .await
        .expect("load")
        .metadata()
        .current_schema()
        .clone();

    const N: i64 = 4;
    let mut handles = Vec::new();
    for k in 0..N {
        let dsn = fx.pg_dsn(&db);
        let whs = whs.clone();
        let cs = cs.clone();
        handles.push(tokio::spawn(async move {
            let catalog = make_catalog(dsn, &whs).await;
            let table = catalog
                .load_table(&TableIdent::new(NamespaceIdent::new("wh".into()), "t".into()))
                .await
                .expect("load");
            append_batches(&catalog, &table, vec![batch(&cs, vec![k * 10, k * 10 + 1])])
                .await
                .expect("append");
        }));
    }
    for h in handles {
        h.await.expect("join");
    }

    let pool: PgPool = fx.pool_for(&db).await;
    // Every mirror snapshot must carry at least one data file (no orphan snapshot
    // rows from a rolled-back CAS attempt), and there must be exactly N of them.
    let snap_count: i64 =
        sqlx::query_scalar("select count(*) from iceberg_mirror.snapshot")
            .fetch_one(&pool).await.expect("count snapshots");
    assert_eq!(snap_count, N, "one snapshot per successful append");
    let orphans: i64 = sqlx::query_scalar(
        "select count(*) from iceberg_mirror.snapshot s \
         where not exists (select 1 from iceberg_mirror.data_file f where f.begin_snapshot = s.snapshot_id)")
        .fetch_one(&pool).await.expect("orphan check");
    assert_eq!(orphans, 0, "no snapshot without its files");
    let files: i64 = sqlx::query_scalar("select count(*) from iceberg_mirror.data_file")
        .fetch_one(&pool).await.expect("count files");
    assert_eq!(files, N, "all appended files present, none duplicated");
}
```

This test needs a `PgFixture::pool_for(db)` returning a `PgPool` for a named db. If it does not exist, add it next to `pg_dsn` in `fixture.rs`:

```rust
/// A fresh sqlx pool for an existing fixture database (read side of a test that
/// also drives the vendored catalog over the same db via `pg_dsn`).
pub async fn pool_for(&self, db: &str) -> PgPool {
    sqlx::PgPool::connect_with(self.opts(db)).await.expect("connect pool_for")
}
```

Add the test target to `src/control-plane/postgres/BUCK`:

```python
loom_fixture_test(
    name = "iceberg-write-roundtrip",
    srcs = ["tests/iceberg_write_roundtrip.rs"],
    crate = "iceberg_write_roundtrip",
    crate_root = "tests/iceberg_write_roundtrip.rs",
    deps = [
        ":postgres",
        "//src/control-plane/core:core",
        "//third-party:iceberg",
        "//third-party:arrow-array-57",
        "//third-party:sqlx",
        "//third-party:tempfile",
        "//third-party:tokio",
    ],
)
```

- [ ] **Step 2: Run it to confirm it fails**

```bash
cd /home/jackm/repos/loom
buck2 test //src/control-plane/postgres:iceberg-write-roundtrip > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|NotFound|assertion" /tmp/t.log | head
```

Expected: FAIL — `current_snapshot` returns `NotFound` (the unmodified `update_table` did not project the mirror).

- [ ] **Step 3: Add the manifest→`ProjectedFile` derivation and columns-absent guard**

In `src/control-plane/postgres/src/iceberg_mirror.rs`, add (the projection structs `ProjectedFile`/`ProjectedColumn` already exist):

```rust
use iceberg::table::Table;

/// Build `ProjectedColumn`s from an Iceberg table's current schema (in-memory).
pub fn columns_of(table: &Table) -> Vec<ProjectedColumn> {
    table
        .metadata()
        .current_schema()
        .as_struct()
        .fields()
        .iter()
        .enumerate()
        .map(|(i, field)| ProjectedColumn {
            order: (i + 1) as i64,
            name: field.name.clone(),
            iceberg_type: match field.field_type.as_ref() {
                iceberg::spec::Type::Primitive(p) => p.to_string(),
                other => panic!("project: non-primitive column type {other:?}"),
            },
            nullable: !field.required,
        })
        .collect()
}

/// Read the data files the table's current snapshot ADDED, as neutral
/// `ProjectedFile`s. Reads the snapshot's manifests via the table's FileIO.
pub async fn added_files_of(table: &Table) -> Result<Vec<ProjectedFile>> {
    let Some(snapshot) = table.metadata().current_snapshot() else {
        return Ok(Vec::new());
    };
    let manifest_list = snapshot
        .load_manifest_list(table.file_io(), table.metadata())
        .await
        .map_err(|e| control_plane_core::Error::Backend(e.to_string()))?;
    let mut files = Vec::new();
    for manifest_file in manifest_list.entries() {
        let manifest = manifest_file
            .load_manifest(table.file_io())
            .await
            .map_err(|e| control_plane_core::Error::Backend(e.to_string()))?;
        for entry in manifest.entries() {
            if entry.snapshot_id() == Some(snapshot.snapshot_id()) {
                let df = entry.data_file();
                files.push(ProjectedFile {
                    path: df.file_path().to_string(),
                    file_format: "parquet".to_string(),
                    record_count: df.record_count() as i64,
                    file_size_bytes: df.file_size_in_bytes() as i64,
                });
            }
        }
    }
    Ok(files)
}

/// True if the mirror has any column row for `table_id` (used to project columns
/// exactly once per table lifetime — slice 2 has no schema evolution).
pub async fn columns_exist(conn: &mut PgConnection, table_id: i64) -> Result<bool> {
    let exists = sqlx::query_scalar!(
        "select exists(select 1 from iceberg_mirror.column where table_id = $1) as \"e!\"",
        table_id,
    )
    .fetch_one(&mut *conn)
    .await
    .map_err(backend)?;
    Ok(exists)
}
```

> Use the exact `control_plane_core::Error` variant the crate already uses for backend errors. Check `backend(...)` in this file (it maps `sqlx::Error` → that variant) and mirror it for the iceberg errors above; if the variant is named differently than `Error::Backend`, adjust both `map_err` closures to match.

- [ ] **Step 4: Project the mirror inside `update_table`**

In `src/control-plane/postgres/src/iceberg_sql_catalog/catalog.rs`, rewrite `update_table` so the CAS and the mirror projection share one transaction. Replace the existing body (from `let update_result = self.execute(...)` through the final `Ok(staged_table)`) with:

```rust
        let mut tx = self.connection.begin().await.map_err(from_sqlx_error)?;

        let update_result = self
            .execute(
                &format!(
                    "UPDATE {CATALOG_TABLE_NAME}
                     SET {CATALOG_FIELD_METADATA_LOCATION_PROP} = ?, {CATALOG_FIELD_PREVIOUS_METADATA_LOCATION_PROP} = ?
                     WHERE {CATALOG_FIELD_CATALOG_NAME} = ?
                      AND {CATALOG_FIELD_TABLE_NAME} = ?
                      AND {CATALOG_FIELD_TABLE_NAMESPACE} = ?
                      AND (
                        {CATALOG_FIELD_RECORD_TYPE} = '{CATALOG_FIELD_TABLE_RECORD_TYPE}'
                        OR {CATALOG_FIELD_RECORD_TYPE} IS NULL
                      )
                      AND {CATALOG_FIELD_METADATA_LOCATION_PROP} = ?"
                ),
                vec![
                    Some(staged_metadata_location),
                    Some(current_metadata_location.as_str()),
                    Some(&self.name),
                    Some(table_ident.name()),
                    Some(&table_ident.namespace().join(".")),
                    Some(current_metadata_location.as_str()),
                ],
                Some(&mut tx),
            )
            .await?;

        if update_result.rows_affected() == 0 {
            // CAS lost: roll the whole unit of work back (pointer + any mirror
            // writes), surface a retryable conflict so iceberg's backoff retries.
            let _ = tx.rollback().await;
            return Err(Error::new(
                ErrorKind::CatalogCommitConflicts,
                format!("Commit conflicted for table: {table_ident}"),
            )
            .with_retryable(true));
        }

        // Project the loom mirror from the canonical staged metadata, in this tx.
        self.project_mirror(&mut tx, &table_ident, &staged_table)
            .await
            .map_err(|e| Error::new(ErrorKind::Unexpected, e.to_string()))?;

        tx.commit().await.map_err(from_sqlx_error)?;
        Ok(staged_table)
```

Add a private helper to the `impl SqlCatalog` block (near `update_table`):

```rust
    /// Project the loom `iceberg_mirror.*` rows for a just-committed table state,
    /// enlisted in the caller's transaction. The namespace key matches the read
    /// path (`TableRef.schema` == `NamespaceIdent::to_url_string`).
    async fn project_mirror(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        ident: &iceberg::TableIdent,
        staged: &iceberg::table::Table,
    ) -> control_plane_core::Result<()> {
        use crate::iceberg_mirror::{
            added_files_of, columns_exist, columns_of, ensure_table, next_snapshot, project_columns,
            project_files,
        };

        let ns = ident.namespace().to_url_string();
        let name = ident.name();
        let iceberg_snap = staged.metadata().current_snapshot().map(|s| s.snapshot_id());
        let files = added_files_of(staged).await?;

        let conn = &mut **tx;
        let at = next_snapshot(conn, iceberg_snap).await?;
        let tid = ensure_table(conn, &ns, name, at).await?;
        if !columns_exist(conn, tid).await? {
            project_columns(conn, tid, at, &columns_of(staged)).await?;
        }
        project_files(conn, tid, at, &files).await?;
        Ok(())
    }
```

Confirm the imports at the top of `catalog.rs` cover `Error`, `ErrorKind` (from `iceberg`), `Transaction`, `Postgres` (already imported per slice 1). Add `use control_plane_core;` paths as needed (fully-qualified above, so no new `use` required).

- [ ] **Step 5: Project drops atomically in `drop_table`**

In the same file, make `drop_table` mark the mirror dropped in the same transaction as the pointer delete. Replace its body so the delete uses `Some(&mut tx)` and, on success, calls `mark_dropped`:

```rust
    async fn drop_table(&self, identifier: &TableIdent) -> Result<()> {
        let mut tx = self.connection.begin().await.map_err(from_sqlx_error)?;
        self.execute(
            &format!(
                "DELETE FROM {CATALOG_TABLE_NAME}
                 WHERE {CATALOG_FIELD_CATALOG_NAME} = ?
                  AND {CATALOG_FIELD_TABLE_NAME} = ?
                  AND {CATALOG_FIELD_TABLE_NAMESPACE} = ?"
            ),
            vec![
                Some(&self.name),
                Some(identifier.name()),
                Some(&identifier.namespace().join(".")),
            ],
            Some(&mut tx),
        )
        .await?;

        let ns = identifier.namespace().to_url_string();
        let conn = &mut *tx;
        let at = crate::iceberg_mirror::next_snapshot(&mut **conn, None)
            .await
            .map_err(|e| Error::new(ErrorKind::Unexpected, e.to_string()))?;
        crate::iceberg_mirror::mark_dropped(&mut **conn, &ns, identifier.name(), at)
            .await
            .map_err(|e| Error::new(ErrorKind::Unexpected, e.to_string()))?;

        tx.commit().await.map_err(from_sqlx_error)?;
        Ok(())
    }
```

> If the existing `drop_table`'s `DELETE` text differs (column names, extra predicates), keep the existing SQL verbatim and only change the `transaction` argument from `None` to `Some(&mut tx)` and append the mirror-mark + commit. Do not invent SQL — read the current method first.

- [ ] **Step 6: Regenerate the sqlx cache**

```bash
cd /home/jackm/repos/loom
./tools/sqlx-prepare.sh
```

Expected: new cache entry for the `columns_exist` `exists(...)` query.

- [ ] **Step 7: Run the round-trip + atomicity tests**

```bash
cd /home/jackm/repos/loom
buck2 test //src/control-plane/postgres:iceberg-write-roundtrip > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log
```

Expected: PASS — both `append_round_trips_through_the_mirror` and `concurrent_appends_keep_the_mirror_consistent`.

- [ ] **Step 8: Clippy + commit**

```bash
cd /home/jackm/repos/loom
buck2 build '//src/control-plane/postgres:postgres[clippy.txt]' 2>&1 | tail -3
git add src/control-plane/postgres/src/iceberg_sql_catalog/catalog.rs src/control-plane/postgres/src/iceberg_mirror.rs src/control-plane/postgres/src/fixture.rs src/control-plane/postgres/tests/iceberg_write_roundtrip.rs src/control-plane/postgres/BUCK src/control-plane/postgres/.sqlx
git commit -m "feat(iceberg): atomic pointer+mirror commit in update_table/drop_table"
```

---

## Task 4: drive the seeder through the real writer

Rewrite the test-support `IcebergWriter` so the catalog contracts exercise the real write path end-to-end: real Parquet + atomic mirror projection. The contracts are the strongest correctness signal (the same ones DuckLake passes).

**Files:**
- Modify: `src/control-plane/postgres/src/fixture.rs` (`IcebergWriter::seed`, `drop_table`)
- Verify: `src/control-plane/postgres/tests/iceberg_catalog.rs` (unchanged) still passes

- [ ] **Step 1: Rewrite `IcebergWriter::seed` to write real batches**

In `src/control-plane/postgres/src/fixture.rs`, replace the body of `IcebergWriter::seed` (the per-batch loop that allocates a snapshot and projects a synthetic `ProjectedFile`) with one that builds a real `RecordBatch` per batch and appends it via `iceberg_writer::append_batches`. The mirror is now projected inside `update_table`, so the seeder no longer calls `next_snapshot`/`ensure_table`/`project_columns`/`project_files` directly. Keep the namespace/table creation block (it already builds the iceberg schema and creates the table). After the table is created and loaded:

```rust
        let table = catalog.load_table(&table_ident).await.expect("load_table");
        let current_schema = table.metadata().current_schema().clone();
        let arrow_schema =
            std::sync::Arc::new(iceberg::arrow::schema_to_arrow_schema(&current_schema).unwrap());

        let mut snapshots = Vec::new();
        for &rows in batches {
            let columns: Vec<std::sync::Arc<dyn arrow_array::Array>> = current_schema
                .as_struct()
                .fields()
                .iter()
                .map(|f| Self::column_array(f, rows))
                .collect();
            let batch = arrow_array::RecordBatch::try_new(arrow_schema.clone(), columns)
                .expect("record batch");
            // Reload the table so each append commits against the latest pointer.
            let table = catalog.load_table(&table_ident).await.expect("reload");
            crate::iceberg_writer::append_batches(&catalog, &table, vec![batch])
                .await
                .expect("append_batches");
            // The loom snapshot id allocated inside update_table is the latest
            // catalog-global id (single-writer seeder).
            let id: i64 = sqlx::query_scalar!(
                "select max(snapshot_id) as \"m\" from iceberg_mirror.snapshot"
            )
            .fetch_one(&self.pool)
            .await
            .expect("max snapshot")
            .expect("at least one snapshot");
            snapshots.push(id);
        }
        snapshots
```

Add the array builder helper to `impl IcebergWriter` (supports the `long`/`string` columns the contracts use — matching the existing `iceberg_type` mapping, which panics on anything else):

```rust
    /// Build a synthetic arrow array of `rows` values for one column, typed to
    /// match its loom-logical → arrow type. Only the types the catalog contracts
    /// seed (`long`, `string`) are supported, mirroring `iceberg_type`.
    fn column_array(
        field: &iceberg::spec::NestedField,
        rows: usize,
    ) -> std::sync::Arc<dyn arrow_array::Array> {
        match field.field_type.as_ref() {
            iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::Long) => {
                std::sync::Arc::new(arrow_array::Int64Array::from(
                    (0..rows as i64).collect::<Vec<_>>(),
                ))
            }
            iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::String) => {
                std::sync::Arc::new(arrow_array::StringArray::from(
                    (0..rows).map(|i| format!("row{i}")).collect::<Vec<_>>(),
                ))
            }
            other => panic!("seed: unsupported column type for real batch: {other:?}"),
        }
    }
```

Remove the now-unused imports from `fixture.rs` (`next_snapshot`, `ensure_table`, `project_columns`, `project_files`, `ProjectedColumn`, `ProjectedFile`) **only if** no other method in the file still uses them — `drop_table` is rewritten next; check the final state before deleting imports.

- [ ] **Step 2: Route the seeder's `drop_table` through the catalog**

Replace `IcebergWriter::drop_table` so it drops via the vendored catalog (which now marks the mirror dropped atomically), instead of calling `mark_dropped` directly:

```rust
    /// Drop a table via the vendored catalog; the mirror is marked dropped in the
    /// same transaction (see `SqlCatalog::drop_table`). Returns the loom snapshot
    /// id at which it was dropped.
    pub async fn drop_table(&self, ns: &str, name: &str) -> i64 {
        let catalog = self.catalog().await;
        let ident = TableIdent::new(NamespaceIdent::new(ns.to_string()), name.to_string());
        catalog.drop_table(&ident).await.expect("drop_table");
        sqlx::query_scalar!("select max(snapshot_id) as \"m\" from iceberg_mirror.snapshot")
            .fetch_one(&self.pool)
            .await
            .expect("max snapshot")
            .expect("at least one snapshot")
    }
```

- [ ] **Step 3: Run the contracts**

```bash
cd /home/jackm/repos/loom
buck2 test //src/control-plane/postgres:iceberg-catalog > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log
```

Expected: PASS — `iceberg_passes_catalog_contract` and `iceberg_passes_catalog_delete_contract`, now backed by real writes.

- [ ] **Step 4: Full postgres sweep + clippy + commit**

```bash
cd /home/jackm/repos/loom
buck2 test //src/control-plane/postgres/... > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log
buck2 build '//src/control-plane/postgres:postgres[clippy.txt]' 2>&1 | tail -3
git add src/control-plane/postgres/src/fixture.rs
git commit -m "test(iceberg): catalog contracts drive the real writer (real Parquet + atomic mirror)"
```

---

## Final verification (after all tasks)

```bash
cd /home/jackm/repos/loom
buck2 test //src/... > /tmp/all.log 2>&1; grep -E "Tests finished|FAIL|Build failure" /tmp/all.log
./tools/clippy-all.sh
buck2 run //tools:prek -- run --all-files
```

**Definition of done:** `buck2 test //src/...` fully green (new writer/roundtrip/seq tests + unchanged contracts + the rest of the tree); `clippy-all.sh` clean; `prek --all-files` clean; `.sqlx` committed and fresh; the real writer produces Parquet readable by an independent reader (the parquet-reader assertion in Task 2).

---

## Self-Review notes (plan ↔ spec)

- **Spec coverage:** Decision 1 (write machinery only, library) → Tasks 2/4 keep it in the postgres crate, no ingest-binary wiring. Decision 2 (single Postgres tx) → Task 3 `update_table`/`drop_table`. Decision 3 (stats deferred) → only `record_count`/`file_size_bytes` projected. Decision 4 (sequence) → Task 1. Decision 5 (append only) → `fast_append`; no replace path. Decision 6 (visibility) → Task 0 (fixup-first, buckify fallback). The v57 blocker section → Task 0. The "atomic chokepoint" section → Task 3. Testing section → Tasks 1/2/3/4 cover concurrency, valid-Parquet, round-trip, atomicity, contracts.
- **Type consistency:** `append_batches(&dyn Catalog, &Table, Vec<RecordBatch>) -> Result<Vec<WrittenFile>>` is defined in Task 2 and called identically in Tasks 3/4. Mirror helpers `columns_of`/`added_files_of`/`columns_exist` are defined in Task 3 Step 3 and called in Step 4. `ProjectedColumn`/`ProjectedFile` reused from slice 1. Read path keys (`table_namespace`/`table_name`) match `to_url_string()`/`name()` used in `project_mirror`.
- **Known soft spots flagged for the implementer:** (a) the exact `control_plane_core::Error` backend variant name (Task 3 Step 3 note); (b) preserve the existing `drop_table` SQL verbatim, changing only the tx arg (Task 3 Step 5 note); (c) verify `SQL_CATALOG_PROP_*` re-exports exist (Task 2 Step 3); (d) the bare-`parquet`-alias contingency (Task 0 Step 3).
