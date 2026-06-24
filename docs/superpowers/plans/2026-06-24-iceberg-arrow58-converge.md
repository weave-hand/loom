# Converge the tree on arrow 58 via iceberg-`main` — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move loom's iceberg stack from arrow/parquet 57 to 58 by sourcing iceberg from a pinned `main` commit (git dependency), converging the whole tree onto a single arrow major.

**Architecture:** A coupled dependency-major bump. iceberg `0.9.1` (crates.io, arrow 57) → iceberg `main` @ pinned SHA (arrow 58) via a git dependency — the tree's first git source. In the same atomic change, the first-party crates that share iceberg's arrow types (`control-plane-postgres`, `engine`, `engine-wire`, `worker`) drop their arrow-57 pins/renames and move to 58, so the `arrow-*-57` / `parquet-57` third-party target families collapse into the single 58 family. The build will not compile in an intermediate half-state, so the manifest+BUCK flip happens first (one task), then compile errors are driven to zero crate-by-crate.

**Tech Stack:** Rust, buck2, reindeer (loom's `weave-hand/reindeer` fork — honors workspace-member dep renames), iceberg-rust, arrow/parquet 58, arrow-flight, sqlx 0.9, DuckDB.

## Global Constraints

- **iceberg pin:** `iceberg = { git = "https://github.com/apache/iceberg-rust", rev = "148afc50ee950b2cd8d99c0243242ef45f3948c5" }` — pinned SHA (iceberg-rust `main` HEAD as of 2026-06-24, confirmed to pin arrow/parquet `58`). No `version` field — a git dep resolves by source, and main's workspace version (`0.9.0`) would not satisfy a `"0.9.1"` constraint. If a fresher pin is wanted, take the current `main` HEAD via `gh api repos/apache/iceberg-rust/commits/main --jq .sha` and re-confirm its `Cargo.toml` still pins arrow `58`.
- **Acceptance gate is the FULL suite:** `buck2 test //src/...` green — never per-crate. The `reindeer update`/`duckdb`-downgrade footgun lands failures in crates the diff never touched (`query-api`/`worker` fixture tests). A green per-crate build is not sufficient evidence.
- **Hold the `duckdb 1.10503.1` pin.** After any lockfile regeneration, diff `Cargo.lock` against `origin/main` for native/`links` crates (`libduckdb-sys`, `zstd-sys`, `ring`); if `duckdb` moved off `1.10503.1`, run `cargo update -p duckdb --precise 1.10503.1` (hermetic cargo) and re-buckify.
- **`.sqlx` cache must stay fresh** — the `//src/control-plane/postgres:sqlx-cache-check` test runs in the full sweep. This change touches no SQL, so the cache should be untouched; if the test fails, regenerate with `tools/sqlx-prepare.sh` and commit.
- **Clean lint:** `tools/clippy-all.sh` clean; `buck2 run //tools:rustfmt -- --check` clean on touched files.
- **Hermetic toolchain for all cargo/buckify:** prefix cargo/reindeer work with `eval "$(./tools/env.sh)"` so the pinned toolchain is on PATH. Never use host rustup.
- **Behavior-preserving:** No new features, no behavior changes. The existing test suite is the regression characterization — do not add behavioral tests; do not delete or weaken existing ones.
- **No piping `buck2 test`/`bxl` through `tail`/`head`** — redirect to a file and grep it.

---

## File-change map

**iceberg-consuming crates (which need the git dep vs. only an API-delta build).** Only three crates declare `iceberg` in their `Cargo.toml` (`grep -rn 'iceberg = ' --include=Cargo.toml src` → worker, postgres, engine); those drive reindeer to emit the iceberg git source. **But** `ingest`, `runtime`, `transform`, and `query-api` also `use iceberg::` in source — they dep on `//third-party:iceberg` directly in their hand-written BUCK (no Cargo.toml entry needed for a first-party buck2 dep). They take whatever iceberg version reindeer emits, so they need **no manifest edit** but **do** need an API-delta compile pass (Task 4).

**Manifests (Cargo.toml):**
- `src/control-plane/postgres/Cargo.toml` — iceberg→git; drop arrow-57 renames (`parquet57`/`arrow-ipc57`/`arrow-select57`) → bare 58; `arrow-array`/`arrow-schema` 57→58; rewrite the explanatory comment block (lines ~29-61).
- `src/services/engine/Cargo.toml` — iceberg→git (line 19); `arrow-flight = "=57.3.1"` → `"58"` (line 22); **dev-deps `arrow-array = "57"`/`arrow-schema = "57"` → `"58"` (lines 27-28)**.
- `src/services/engine-wire/Cargo.toml` — `arrow-flight = "=57.3.1"` → `"58"` (line 23; does not depend on iceberg directly).
- `src/services/worker/Cargo.toml` — iceberg→git (line 21); dev-deps `arrow-array`/`arrow-schema` 57→58 (lines 17-18).

**Generated:**
- `Cargo.lock` — git source for iceberg, single arrow 58.
- `third-party/BUCK` — `git_fetch` iceberg; `arrow-*-57` / `parquet-57` families gone; bare aliases (`arrow-array`, `arrow-schema`, `arrow-flight`, `parquet`, `arrow-ipc`, `arrow-select`) all resolve to 58.

**First-party BUCK:**
- `src/control-plane/postgres/BUCK` — remove all `named_deps` entries referencing `parquet57`/`arrow_ipc57`/`arrow_select57` (main lib lines ~183-191 + ~12 test targets); add the bare third-party dep to the relevant `deps` where the source now `use`s the bare crate (the exact alias reindeer emits is verified in Task 2 — expected `//third-party:parquet`, `//third-party:arrow-ipc`, `//third-party:arrow-select`, but these bare aliases do **not** exist today and are only emitted once postgres declares the deps bare).
- `src/services/query-api/BUCK` — remove the `arrow_ipc57` named_dep on the `iceberg-schema-evolution-read` test (line ~526); add the bare arrow-ipc alias to its deps.
- `src/services/worker/BUCK` — remove the `arrow_ipc57` named_dep on the `flight-roundtrip` test (line ~98); add the bare arrow-ipc alias to its deps.
- `src/services/engine/BUCK` — unchanged in wiring (already uses bare `arrow-array`/`arrow-flight`, which now resolve to 58).

**Source (query-api):** `src/services/query-api/tests/iceberg_schema_evolution_read.rs:10` — `arrow_ipc57` → `arrow_ipc`.

**Source (import rewrites — bare alias replaces `*57`):**
- `parquet57` → `parquet` (12 refs): `iceberg_stats.rs` (5), `iceberg_writer.rs`, `iceberg_read.rs`, `iceberg_inline.rs`, `tests/iceberg_inline.rs`, `tests/iceberg_writer.rs`, `tests/iceberg_column_stats_unit.rs` (2).
- `arrow_ipc57` → `arrow_ipc` (7 refs): `iceberg_landing.rs`, `tests/iceberg_read.rs`, `tests/iceberg_landing.rs`, `tests/iceberg_overwrite.rs`, `tests/iceberg_schema_evolution_land.rs`, `tests/vector_landing.rs`, `worker/tests/flight_roundtrip.rs`.
- `arrow_select57` → `arrow_select` (1 ref): `iceberg_landing.rs`.

**Source (iceberg API delta — compile-driven, Task 4):** `iceberg_writer.rs`, `iceberg_landing.rs`, `iceberg_mirror.rs`, `iceberg_read.rs`, `iceberg_stats.rs`, `iceberg_sql_catalog/{catalog.rs,s3_storage.rs,error.rs,mod.rs}`, `iceberg_inline.rs`, `iceberg_flush.rs`, `iceberg_catalog.rs`, `iceberg_control_plane.rs`, `iceberg_schema_evolution.rs`, `iceberg_type.rs`.

**Docs:**
- `CLAUDE.md` — document the iceberg git pin + bump procedure in the third-party section.
- Registers via `loom-docs-update` at finish (Task 6).

---

### Task 1: Flip manifests to iceberg-`main` + arrow 58, regenerate the dependency graph

This is a single config task: every manifest moves at once because the tree will not compile in a half-flipped state. Its deliverable is a regenerated `third-party/BUCK` whose iceberg is git-sourced and whose arrow is a single major 58. Compilation of first-party crates is the job of Tasks 2–5; here we only prove the **graph resolves** the way we intend.

**Files:**
- Modify: `src/control-plane/postgres/Cargo.toml` (lines ~29-61), `src/services/engine/Cargo.toml:19,22`, `src/services/engine-wire/Cargo.toml:23`, `src/services/worker/Cargo.toml:17-18,21`
- Modify (generated): `Cargo.lock`, `third-party/BUCK`

**Interfaces:**
- Produces: the bare third-party aliases `//third-party:{arrow-array,arrow-schema,arrow-ipc,arrow-select,parquet,arrow-flight}` all resolving to version 58; a `git_fetch` rule for `//third-party:iceberg` at the pinned SHA. Tasks 2–5 consume these.

- [ ] **Step 1: Point the three iceberg consumers at the git pin**

In `src/control-plane/postgres/Cargo.toml`, `src/services/engine/Cargo.toml`, and `src/services/worker/Cargo.toml`, replace each `iceberg = "0.9"` line with:

```toml
iceberg = { git = "https://github.com/apache/iceberg-rust", rev = "148afc50ee950b2cd8d99c0243242ef45f3948c5" }
```

Preserve any per-crate `features`/`default-features` already on the iceberg dep (none today, but check). Leave the surrounding explanatory comments for now except where they assert "iceberg 0.9 pins arrow 57" — update that wording in Step 2.

- [ ] **Step 2: Remove the arrow-57 renames and pins**

In `src/control-plane/postgres/Cargo.toml`, replace the renamed 57 deps with bare 58 deps and rewrite the comment block (~lines 29-61) to describe the single-major state. Concretely:

```toml
arrow-array = "58"
arrow-schema = "58"
parquet = { version = "58", default-features = false, features = ["arrow"] }
arrow-ipc = { version = "58", default-features = false }
arrow-select = { version = "58", default-features = false }
```

(Drop the `package = "parquet"` / `package = "arrow-ipc"` / `package = "arrow-select"` renames entirely — with one arrow major there is no alias collision to dodge, so the `weave-hand/reindeer#1` rename trick is no longer needed here.) Replace the now-stale comment paragraphs with a 2-3 line note: iceberg-`main` is on arrow/parquet 58, matching DataFusion 54 / DuckDB, so these are bare 58 deps shared across the tree.

In `src/services/engine/Cargo.toml` and `src/services/engine-wire/Cargo.toml`, change `arrow-flight = "=57.3.1"` to:

```toml
arrow-flight = "58"
```

In **both** `src/services/engine/Cargo.toml` (lines 27-28) and `src/services/worker/Cargo.toml` (lines 17-18), change the dev-deps `arrow-array = "57"` / `arrow-schema = "57"` to `"58"`. (Missing the engine dev-deps would keep the `arrow-array-57`/`arrow-schema-57` families alive and fail the collapse check.)

- [ ] **Step 3: Regenerate the lockfile under the hermetic toolchain**

Run:

```bash
cd /workspace && eval "$(./tools/env.sh)" && cargo generate-lockfile
```

Expected: `Cargo.lock` gains `source = "git+https://github.com/apache/iceberg-rust?rev=148afc50...#148afc50..."` for `iceberg` (and its iceberg-rust sibling member crates if any are pulled), and the `arrow-*`/`parquet` entries collapse to a single `58.x` set.

- [ ] **Step 4: Guard the duckdb pin**

A lock downgrade changes only the `version = "..."` line, not the `name = "duckdb"` line (lock ordering is stable/alphabetical), so assert the version directly rather than diffing the `name` line:

```bash
cd /workspace
grep -A1 'name = "duckdb"' Cargo.lock | grep -q 'version = "1.10503.1"' && echo "duckdb pin OK" || echo "DUCKDB MOVED — fix"
# Also eyeball the other native/links crates for churn:
git diff origin/main -- Cargo.lock | grep -E '^[+-]version' -B1 | grep -iE 'duckdb|zstd-sys|ring' && echo "(review the above)" || echo "no native-crate version churn"
```

If duckdb is not pinned at `1.10503.1`:

```bash
eval "$(./tools/env.sh)" && cargo update -p duckdb --precise 1.10503.1 && ./tools/buckify.sh
```

Expected: `duckdb pin OK`.

- [ ] **Step 5: Regenerate third-party/BUCK**

Run:

```bash
cd /workspace && ./tools/buckify.sh
```

Expected output ends with `buckify complete`. (reindeer handles the git source natively — it emits a `git_fetch` rule; the archive-before-buildscript ordering is reindeer's native rule-priority sort, PR #102, which covers git_fetch too, so no post-processing is needed.)

- [ ] **Step 6: Verify the graph flipped as intended**

Run:

```bash
cd /workspace
echo "--- iceberg git source present? ---"; grep -n "git_fetch\|iceberg-rust" third-party/BUCK | head
echo "--- arrow-57 / parquet-57 families gone? ---"; grep -cE 'name = "(arrow-[a-z]+|parquet)-57' third-party/BUCK
echo "--- bare arrow aliases now point to 58? ---"; grep -nE 'name = "arrow-array"|name = "parquet"|name = "arrow-flight"' -A2 third-party/BUCK | grep -E '57|58'
```

Expected: a `git_fetch` rule referencing iceberg-rust exists; the `-57` family count is **0**; the bare `arrow-array`/`parquet`/`arrow-flight` aliases resolve to `-58` targets. If any `-57` target remains, a first-party crate still pins 57 — fix its manifest and re-buckify before proceeding.

Also confirm reindeer's native rule-priority sort placed the iceberg `git_fetch` ahead of any iceberg `buildscript_run` (iceberg-rust members may carry build scripts — the spec flagged this as a risk):

```bash
cd /workspace && awk '/git_fetch|buildscript_run/{print NR": "$0}' third-party/BUCK | grep -iE 'iceberg' || echo "(no iceberg buildscript_run — nothing to order)"
```

If a `buildscript_run` for an iceberg member appears *before* its `git_fetch`, that's the ordering bug the spec warned about — extend `tools/buckify.sh` with the reorder. (Expected: none, or git_fetch first.)

- [ ] **Step 7: Commit**

```bash
cd /workspace && git add Cargo.lock third-party/BUCK \
  src/control-plane/postgres/Cargo.toml \
  src/services/engine/Cargo.toml src/services/engine-wire/Cargo.toml src/services/worker/Cargo.toml
git commit -m "build(iceberg): source iceberg from main @148afc50, flip deps to arrow 58"
```

---

### Task 2: Rewrite postgres source imports `*57` → bare, fix postgres BUCK wiring

With the graph on one major, the `*57` aliases no longer exist. Rewrite the postgres crate's imports to the bare crate names and drop the `named_deps` that mapped them.

**Files:**
- Modify: `src/control-plane/postgres/BUCK` (main lib `named_deps` ~183-191; test targets at ~251, 506, 525, 546, 567, 602, 638, 678, 698, 719, 753, 806)
- Modify: `src/control-plane/postgres/src/{iceberg_stats.rs,iceberg_writer.rs,iceberg_read.rs,iceberg_inline.rs,iceberg_landing.rs}` and `src/control-plane/postgres/tests/{iceberg_inline.rs,iceberg_writer.rs,iceberg_column_stats_unit.rs,iceberg_read.rs,iceberg_landing.rs,iceberg_overwrite.rs,iceberg_schema_evolution_land.rs,vector_landing.rs}`

**Interfaces:**
- Consumes: bare `//third-party:{parquet,arrow-ipc,arrow-select}` (Task 1).
- Produces: a postgres crate whose only blocker to compiling is the iceberg API delta (Task 4).

- [ ] **Step 1: Rewrite the source imports**

Replace the renamed-crate identifiers with the bare crate names throughout the postgres source and tests:

```bash
cd /workspace
grep -rl 'parquet57\|arrow_ipc57\|arrow_select57' src/control-plane/postgres/src src/control-plane/postgres/tests \
  | xargs sed -i 's/\bparquet57\b/parquet/g; s/\barrow_ipc57\b/arrow_ipc/g; s/\barrow_select57\b/arrow_select/g'
```

Then read each touched file to confirm the rewrite is sane (no `parquet` already meaning something else; the `iceberg_stats.rs` module doc comment at the top still mentions "parquet57" in prose — update that prose to say the stats are now computed against the same `parquet` 58 as the rest of the tree, and note the duplicated merge logic in `datafusion_io::write::file_stats_from_bytes` can now potentially be unified — but DO NOT unify it in this task; leave a `// TODO(fut)` only if one is warranted, otherwise just correct the comment).

- [ ] **Step 2: Drop the `*57` named_deps in postgres/BUCK and add bare deps**

First, confirm the exact bare alias names reindeer emitted once postgres declares the deps bare (these aliases did not exist while the renames were in place):

```bash
cd /workspace && grep -nE 'name = "arrow-ipc"|name = "arrow-select"|name = "parquet"' third-party/BUCK
```

Then, in `src/control-plane/postgres/BUCK`, remove every `named_deps` map entry keyed `parquet57` / `arrow_ipc57` / `arrow_select57` (the main lib map at ~183-191 plus each test target listed in Files). Where removing leaves a target with no access to the crate it now `use`s by bare name, add the bare third-party dep (using the exact alias from the grep above) to that target's `deps`:
- targets whose source uses `parquet::` → add `"//third-party:parquet"`
- targets whose source uses `arrow_ipc::` → add `"//third-party:arrow-ipc"`
- targets whose source uses `arrow_select::` → add `"//third-party:arrow-select"`

If reindeer did not emit a bare `arrow-ipc`/`arrow-select` alias, it means those deps are not declared as direct bare deps — re-check Task 1 Step 2 dropped the `package = ...` renames so postgres asks for plain `arrow-ipc = "58"` / `arrow-select = "58"`.

- [ ] **Step 3: Verify no `*57` identifiers remain in postgres**

```bash
cd /workspace && grep -rn 'parquet57\|arrow_ipc57\|arrow_select57' src/control-plane/postgres && echo "FOUND — fix" || echo "clean"
```

Expected: `clean`.

- [ ] **Step 4: Commit**

```bash
cd /workspace && git add src/control-plane/postgres
git commit -m "refactor(iceberg): bare arrow/parquet 58 imports in postgres (drop *57 renames)"
```

---

### Task 3: Worker + query-api + engine arrow-flight/arrow-58 wiring

**Files:**
- Modify: `src/services/worker/BUCK` (`flight-roundtrip` test ~98), `src/services/worker/tests/flight_roundtrip.rs:45`
- Modify: `src/services/query-api/BUCK` (`iceberg-schema-evolution-read` test ~526), `src/services/query-api/tests/iceberg_schema_evolution_read.rs:10`
- Modify (if needed): `src/services/engine/BUCK`, `src/services/engine-wire/` Flight codec sources

**Interfaces:**
- Consumes: `//third-party:arrow-flight` (now 58), the bare arrow-ipc alias (Task 1/2).
- Produces: worker/query-api/engine/engine-wire crates compiling against arrow 58.

- [ ] **Step 1: Rewrite the worker + query-api flight/IPC test imports + BUCK**

In `src/services/worker/tests/flight_roundtrip.rs:45`, rewrite `arrow_ipc57` → `arrow_ipc`. In `src/services/worker/BUCK`, remove the `named_deps = {"arrow_ipc57": ...}` on the `flight-roundtrip` test and add the bare arrow-ipc alias to its `deps`.

In `src/services/query-api/tests/iceberg_schema_evolution_read.rs:10`, rewrite `arrow_ipc57` → `arrow_ipc`. In `src/services/query-api/BUCK`, remove the `named_deps = {"arrow_ipc57": ...}` on the `iceberg-schema-evolution-read` test (line ~526) and add the bare arrow-ipc alias to its `deps`.

- [ ] **Step 2: Build the engine/engine-wire/worker crates, fix arrow-flight-58 churn**

```bash
cd /workspace && buck2 build //src/services/engine:engine //src/services/engine-wire:engine-wire //src/services/worker:worker > /tmp/eng.log 2>&1; grep -E "BUILD (SUCCEEDED|FAILED)|error\[" /tmp/eng.log
```

For each compile error, fix the Flight encode/decode call site per the arrow-flight 58 API (the encode/decode of `RecordBatch`/`Schema` over Flight is the likely churn point — e.g. `FlightDataEncoderBuilder`, `flight_data_to_arrow_batch`, IPC writer options). These are arrow-major API moves, not behavior changes. Re-run until `BUILD SUCCEEDED`.

- [ ] **Step 3: Commit**

```bash
cd /workspace && git add src/services/worker src/services/query-api src/services/engine src/services/engine-wire
git commit -m "build(engine): arrow-flight 58 wiring (worker/query-api flight tests, codec churn)"
```

---

### Task 4: Absorb the iceberg `0.9.1 → main` API delta in postgres

The iceberg API delta is **discovered by compiling**, not predicted — main's workspace version is `0.9.0` (≈ the published `0.9.1`), so the delta is expected to be small (mostly arrow-58 types flowing through the writer/reader bridges), but every call site below must be checked against the build. Do NOT guess signatures; let the compiler name each break and fix it against iceberg-rust's source/docs at the pinned SHA.

**Files:**
- Modify (as compile errors dictate): `src/control-plane/postgres/src/iceberg_writer.rs`, `iceberg_landing.rs`, `iceberg_mirror.rs`, `iceberg_read.rs`, `iceberg_stats.rs`, `iceberg_inline.rs`, `iceberg_flush.rs`, `iceberg_catalog.rs`, `iceberg_control_plane.rs`, `iceberg_schema_evolution.rs`, `iceberg_type.rs`, `iceberg_sql_catalog/{catalog.rs,s3_storage.rs,error.rs,mod.rs}`
- Modify (Step 3, as compile errors dictate): any `iceberg::`-using source in `src/services/{ingest,runtime,transform,query-api}`

**Interfaces:**
- Consumes: iceberg `main` API at SHA `148afc50`.
- Produces: `//src/control-plane/postgres:postgres` and the other iceberg-consuming crates compile clean.

**Call-site inventory to verify against the new API** (from the codebase map — check each compiles, fix per the new signature if not):
- **Writer chain** (`iceberg_writer.rs:14-20, 229-250`): `DataFileWriterBuilder::new(rolling).build(None)`, `ParquetWriterBuilder::new(WriterProperties::default(), schema)`, `RollingFileWriterBuilder::new_with_default_file_size(...)`, `DefaultLocationGenerator::new(table.metadata().clone())`, `DefaultFileNameGenerator::new(prefix, None, DataFileFormat::Parquet)`, `IcebergWriter::{write,close}`.
- **Transaction/commit** (`iceberg_writer.rs:59-62`): `Transaction::new(table)`, `tx.fast_append().add_data_files(data_files)`, `ApplyTransactionAction::apply(tx)`, `tx.commit(catalog)`.
- **Catalog trait impl** (`iceberg_sql_catalog/catalog.rs:132-176, 513-1117`): `CatalogBuilder` (`with_storage_factory`, `load`), the full `Catalog` trait (`load_table`, `create_table`, `update_table(TableCommit)`, etc.), `commit.apply(current_table)` (`:414`), `metadata_location_result()`, `metadata().write_to(file_io, loc)`, `MetadataLocation::new_with_table_location`, `TableMetadata::{read_from,write_to}`, `TableMetadataBuilder::from_table_creation`, `Table::builder()...build()`.
- **Storage traits** (`s3_storage.rs:92-273`): `#[typetag::serde] impl StorageFactory` (`build(&StorageConfig)`), `impl Storage` (the full method set incl. `new_input`/`new_output`/`reader`/`writer`/`delete_prefix`), `impl FileRead`/`FileWrite`. Watch for any `StorageConfig`/`StorageFactory` signature change — this is loom-vendored code that must match the new trait exactly.
- **Spec types** (`iceberg_landing.rs:20,590-615`, `iceberg_mirror.rs:249-268`): `Schema::builder().with_fields(...).build()`, `NestedField::{optional,required,list_element}`, `Type::{Primitive,List}`, `PrimitiveType`, `ListType::new`, `schema.as_struct().fields()`.
- **Arrow bridge** (`iceberg_read.rs:41-43`, `iceberg_landing.rs:209-211,306`): `iceberg::arrow::schema_to_arrow_schema(schema)` — its return type is now arrow-58 `Schema`; ensure the `Arc<Schema>`/`SchemaRef` it feeds are arrow-58 types everywhere downstream.
- **Mirror/manifest loaders** (`iceberg_mirror.rs:281-317`): `metadata().current_snapshot()`, `snapshot.load_manifest_list(file_io, metadata)`, `manifest_file.load_manifest(file_io)`, `manifest.entries()`, `entry.{snapshot_id,data_file}`, `df.file_path()`, `file_io().new_input(path).read()`.
- **Error** (`iceberg_sql_catalog/error.rs:18-48`): `Error::new(ErrorKind::Unexpected, msg).with_source(e)`, `ErrorKind::CatalogCommitConflicts`, `.with_retryable(true)`.
- **Stats footer reader** (`iceberg_stats.rs`): now reads parquet-58 footers — the `parquet::file::statistics::Statistics` enum is the 58 variant; the merge logic must match the 58 `Statistics` shape.

- [ ] **Step 1: Compile postgres, capture the error list**

```bash
cd /workspace && buck2 build //src/control-plane/postgres:postgres > /tmp/pg.log 2>&1; grep -E "BUILD (SUCCEEDED|FAILED)|error\[|error:" /tmp/pg.log | head -60
```

- [ ] **Step 2: Fix each error against the pinned iceberg API**

For each `error[...]`, open the named file/line and adjust the call to the iceberg-`main` signature. Authoritative source for the new signatures: the iceberg-rust checkout at the pinned SHA (`buck2`'s git_fetch materializes it under buck-out; or read on GitHub at `apache/iceberg-rust` @ `148afc50`), and `https://docs.rs` is NOT valid here (unpublished) — use the repo source. Keep each fix minimal and behavior-preserving. Re-run Step 1 until `BUILD SUCCEEDED`.

- [ ] **Step 3: Build the other iceberg-consuming crates and fix their delta**

`ingest`, `runtime`, `transform`, and `query-api` `use iceberg::` (via `//third-party:iceberg` in their BUCK, no Cargo.toml entry) and so face the same API delta — build them and absorb any break with the same compile-driven approach against the pinned-SHA source:

```bash
cd /workspace && buck2 build //src/services/ingest:ingest //src/services/runtime:runtime //src/services/transform:transform //src/services/query-api:query-api > /tmp/rest.log 2>&1; grep -E "BUILD (SUCCEEDED|FAILED)|error\[|error:" /tmp/rest.log | head -40
```

Most of these consume iceberg only through postgres's public surface, so breaks are likely few or none — but do not assume; drive to `BUILD SUCCEEDED`.

- [ ] **Step 4: Clippy-clean the touched crates**

```bash
cd /workspace && for t in '//src/control-plane/postgres:postgres' '//src/services/ingest:ingest' '//src/services/transform:transform' '//src/services/query-api:query-api'; do buck2 build "${t}[clippy.txt]" --show-output > /tmp/cl.log 2>&1; OUT=$(grep -oE '/[^ ]+clippy.txt' /tmp/cl.log | head -1); test -s "$OUT" && { echo "LINT in $t:"; cat "$OUT"; } || echo "clippy clean: $t"; done
```

Expected: `clippy clean` for each. Fix any lint the API churn introduced.

- [ ] **Step 5: Commit**

```bash
cd /workspace && git add src/control-plane/postgres src/services/ingest src/services/runtime src/services/transform src/services/query-api
git commit -m "fix(iceberg): absorb iceberg main API delta (0.9.1 -> 148afc50)"
```

---

### Task 5: Full-suite regression gate + collapse verification

**Files:** none (verification + any fallout fixes land in the relevant crate)

- [ ] **Step 1: Run the full test suite**

```bash
cd /workspace && buck2 test //src/... > /tmp/full.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/full.log | tail -40
```

Expected: `Tests finished: Pass N. Fail 0`. The fixture-backed iceberg tests (`iceberg-writer`, `iceberg-landing`, `iceberg-read`, `iceberg-overwrite`, `iceberg-schema-evolution-land`, `iceberg-column-stats`, `iceberg-inline`, `iceberg-flush`, `inline-flush-trigger`, `vector-landing`, the S3/MinIO round-trip, and `worker:flight-roundtrip`) are the real regression signal — they exercise the write/read/Flight paths end-to-end. Investigate any failure with systematic-debugging; do not weaken a test to make it pass.

- [ ] **Step 2: Confirm the arrow-major collapse**

```bash
cd /workspace
echo "57 families remaining (want 0):"; grep -cE 'name = "(arrow-[a-z]+|parquet)-57' third-party/BUCK
echo "stray *57 aliases in first-party (want none):"; grep -rn 'parquet57\|arrow_ipc57\|arrow_select57\|arrow-flight.*57' src --include=*.rs --include=BUCK || echo none
```

Expected: `0` and `none`.

- [ ] **Step 3: Lint sweep**

```bash
cd /workspace && ./tools/clippy-all.sh > /tmp/clippy.log 2>&1; tail -5 /tmp/clippy.log
```

Expected: clean. Then `buck2 run //tools:prek -- run --all-files` and commit anything the hooks fix.

- [ ] **Step 4: Commit any fallout**

```bash
cd /workspace && git add -A && git commit -m "test(iceberg): green full suite on arrow 58" || echo "nothing to commit"
```

---

### Task 6: Document the pin + close the register item

**Files:**
- Modify: `CLAUDE.md` (third-party Rust deps section)
- Modify: `docs/ROADMAP.md` (via `loom-docs-update`)

- [ ] **Step 1: Document the iceberg git pin in CLAUDE.md**

In the **Third-party Rust deps** section of `CLAUDE.md`, add a short note: iceberg is sourced from a pinned `main` SHA (git dep — the tree's only git source) carrying arrow/parquet 58, ahead of the post-0.9.1 crates.io release; the whole tree is on a single arrow major (58). Bump procedure: change the `rev` in the three consuming `Cargo.toml`s, `cargo generate-lockfile` (guard the `duckdb 1.10503.1` pin), `./tools/buckify.sh`. Exit when iceberg publishes: swap git dep → `version = "0.x"`, arrow stays 58.

- [ ] **Step 2: Close the register item**

Invoke the `loom-docs-update` skill to flip `road-iceberg-arrow58-converge` to `- [x] status:done` and add `pr:#<N>` once the PR number is known (do this in the finishing step). Also confirm `fut-iceberg-arrow58-converge` is already `status:promoted` (it is) and that no new deferral was introduced by this work (if the `iceberg_stats.rs` duplicated-merge-logic unification was left as a follow-up, record it as a new FUTURE item then).

- [ ] **Step 3: Validate + commit**

```bash
cd /workspace && bash tools/docs.sh validate && git add CLAUDE.md docs/ROADMAP.md docs/FUTURE.md && git commit -m "docs(iceberg): document iceberg main pin; close road-iceberg-arrow58-converge"
```

---

## Self-Review

This plan was gated by an adversarial plan-review subagent; the findings below (F1 query-api, F2 engine dev-deps, F3 duckdb-guard grep, F4 ingest/runtime/transform delta, F5 non-existent bare aliases) were all confirmed against the repo and folded in before implementation.

**Authoritative `*57` inventory** (every hit must map to a task; verified via `grep -rnE 'parquet57|arrow_ipc57|arrow_select57|arrow-array = "57"|arrow-schema = "57"|arrow-flight = "=?57' src --include=*.rs --include=Cargo.toml --include=BUCK`):
- postgres: manifest renames + ~13 source/test files + ~13 BUCK named_deps → Tasks 1, 2, 4. ✓
- engine: `arrow-flight`, dev-deps `arrow-array`/`arrow-schema` → Task 1 Step 2. ✓
- engine-wire: `arrow-flight` → Task 1 Step 2. ✓
- worker: dev-deps + `flight_roundtrip.rs:45` `arrow_ipc57` + `BUCK:98` → Tasks 1, 3. ✓
- query-api: `tests/iceberg_schema_evolution_read.rs:10` `arrow_ipc57` + `BUCK:526` → Task 3. ✓

**Spec coverage:**
- Sourcing via pinned git dep (no floating branch) → Global Constraints + Task 1 (Steps 1, 3, 5). ✓
- Arrow 57→58 converge across all consumers (postgres/engine/engine-wire/worker/query-api) → Tasks 1–4. ✓
- `arrow-flight` pin comes off → Task 1 Step 2, Task 3. ✓
- Flight codec, stats footer reader, iceberg 0.9→main API delta (incl. ingest/runtime/transform/query-api) → Task 3, Task 4. ✓
- `arrow-*-57`/`parquet-57` families collapse → Task 1 Step 6, Task 5 Step 2. ✓
- Full-suite gate, duckdb pin guard (version-keyed), sqlx freshness, clippy → Global Constraints + Task 5. ✓
- Exit-when-published note → Task 6 Step 1. ✓

**Placeholder scan:** Task 4 is deliberately compile-error-driven — correct for a dependency-major bump where signatures are discovered at build time; it ships the full call-site inventory and the authoritative-source instruction (the pinned-SHA repo source, not docs.rs) so the implementer never guesses. No "TBD"/"handle edge cases"/vague-validation steps elsewhere.

**Type consistency:** The rename removals are consistent (`parquet57`→`parquet`, `arrow_ipc57`→`arrow_ipc`, `arrow_select57`→`arrow_select`) across manifest, BUCK, and source tasks, and the bare third-party alias names are *verified by grep* (Task 2 Step 2) rather than assumed, since they are only emitted after the renames drop. The git-dep form is identical everywhere; the pinned SHA `148afc50ee950b2cd8d99c0243242ef45f3948c5` is used verbatim throughout.
