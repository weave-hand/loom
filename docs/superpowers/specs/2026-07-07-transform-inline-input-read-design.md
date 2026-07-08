# Transform inputs must read the hot inline tier, not cold Parquet only

**Status:** design → build (this PR)
**Area:** transform

**Fixes (this PR):** the transform inline-blindness correctness bug
(`iss-transform-inline-blind`) described below. It lands **fixed** in the same
PR, so — per loom's registers-carry-open-work-only rule — it is recorded in
`docs/system-capabilities/transform.md` and this spec rather than as a standing
`ISSUES.md` row.
**Files one open follow-up:** `#iss-serving-empty-table-not-found` (a genuinely
deferred serving-path gap, added to `ISSUES.md`).

## Problem

A physical or typed transform silently produces an **empty output** whenever any
of its input tables holds rows only in the **hot inline tier** (Postgres
`iceberg_mirror.inline_<id>`) that have not yet been flushed to cold Parquet.

loom's write path lands small datasets **inline** and only flushes them to
Parquet once they exceed `flush_byte_threshold`. The serving read path
(`do_get_sql` → `engine_serving::execute_query_stream`) merges the hot inline
tier with the cold Parquet tier, so object reads, previews, and governed SQL all
see the current logical table. **The transform worker's input read does not.**

`src/services/worker/src/transform.rs` reads each input by:

1. `list_files(schema, name)` — the engine returns the live snapshot's **cold
   Parquet `data_file` set** (`files_with_stats`) plus the declared columns.
2. Fetching those files' bytes over the Flight `Files` ticket (`do_get_files`).
3. Registering the resulting batches in a local DataFusion context; **zero files
   registers as an empty relation** (`transform.rs:252`).

For an inline-only table, step 1 returns an empty file set, so the worker
registers an **empty** input, the SQL runs over nothing, and the transform
commits an output table with a snapshot but no rows.

### Reproduction (verified against a live `dev-up` stack)

Seed `employees` (8 rows) + `departments` (4 rows) — both inline-only, never
flushed (`iceberg_mirror.data_file` empty). Define and run a join transform:

- The join yields **8 rows** when run directly in Postgres against the inline
  tables, and `GET /objects/employees` returns 8 (the serving path merges
  inline).
- The transform run reports **succeeded** with a snapshot, but the output
  `public.employee_floor` has `current-snapshot-id: None`, zero `data_file`
  rows, and no `inline_<id>` table — an empty table.
- `GET /datasets/public/employee_floor/preview` then fails
  `table 'datafusion.public.employee_floor' not found`, because the serving
  catalog does not register a zero-row table (see the follow-up below).

### Why the existing e2e never caught it

`src/services/worker/tests/transform_e2e.rs` seeds inputs with
`InlineLimits { inline_byte_limit: 0, flush_byte_threshold: i64::MAX }` —
`inline_byte_limit: 0` forces every landed row straight to **cold Parquet**. The
suite only ever exercises cold-file inputs, so the inline-input path was never
tested.

### Severity

This is a **silent correctness bug**, not merely a dev-up artifact. In
production, any table with a hot inline tail (recent appends, or COW
inline-shadow UPDATE/DELETE rows not yet consolidated) has those rows **silently
dropped** from every transform that reads it — wrong results, no error. It is
distinct from the *accepted* external-visibility gap `#iss-iceberg-inline-visibility`
(raw-Iceberg clients seeing inline only after flush): the transform worker is an
**internal** component that already has the merged read available and simply does
not use it.

## Decision

**The transform worker reads each input through the engine's merged serving SQL
path, not the cold-file path.** The engine already exposes exactly this via
`FlightSqlClient` (`CommandStatementQuery` → `do_get_sql` →
`execute_query_stream`), which is what query-api's object reads use. **No engine
change is required** — the fix is confined to the worker.

Reusing the serving read has a second correctness benefit: the worker now sees
the table's *logical current contents* — inline appends **and** COW
inline-shadow mutations (latest-shadow-wins, tombstones) — exactly as a governed
reader would, instead of raw base-Parquet rows.

### Why not fix it in the file path

`list_files`/`do_get_files` name explicit Parquet files; the hot tier is
Postgres rows, not files, so it cannot be expressed as a file ticket without the
worker gaining Postgres access — which the zero-pool invariant forbids
(`transform.md` → Worker execution model). The merged read must happen
engine-side, and `do_get_sql` already does it.

### Compaction is deliberately unaffected

`compact_table` coalesces small **Parquet** files into larger ones; inline rows
are not its concern (the separate `flush_table` job materializes inline → Parquet).
Compaction correctly keeps reading the cold file set via `list_files` +
`do_get_files`. Only transforms — which need the table's logical rows — move to
the merged read. This intentionally breaks the previous compaction/transform
read symmetry.

## Design

### Worker changes only

`TransformCtx` gains a `sql: FlightSqlClient` (a second zero-pool client over the
same engine UDS). The shared input-registration loop in `run_wire_transform`
changes its **data source** while keeping the existing existence/empty-schema
oracle:

- Keep `list_files` for two things only: **existence** (`columns == None` ⇒
  unknown table ⇒ `Abandon`, unchanged) and the **declared column schema** used
  to register an empty input.
- Replace the cold-file fetch with a merged read:
  `ctx.sql.execute("SELECT * FROM \"<schema>\".\"<name>\"")`. Identifiers are
  double-quoted (and embedded quotes doubled) so the physical `schema.name`
  resolves against the serving catalog's `TableReference::partial(schema, name)`
  registration.
- The read is by **physical `TableRef`**; the batches are still registered under
  `register_as` (the physical table name, or the ontology type name for typed
  transforms) — the resolution that maps a typed input's type to its backing
  table is unchanged and upstream of this loop, so both physical and typed
  transforms are fixed by this one change.

### The empty-input edge

A live-but-**empty** table (no inline rows and no cold files — e.g. the existing
`empty_input_*` fixture, or a freshly-created output used as a downstream input)
is **not registered** in the serving catalog (`serving.rs:176`,
`(None, None) => Ok(None)`), so `SELECT * FROM it` plan-errors. That maps through
`FlightSqlClient` → `sql_status` → `ControlPlaneError::Validation`. Because
`list_files` has already proven the table exists (`columns` is `Some`), the
worker treats a `Validation` error on the input read as **"exists but empty"**
and registers an empty relation with the declared schema — preserving today's
"the SQL runs over an empty input" semantics and keeping the existing empty-input
edge tests green.

- `Ok(batches)` non-empty → `register_batches`.
- `Ok(empty)` (0 batches) → `register_empty_table` (declared schema).
- `Err(Validation)` (table exists per `list_files` but unregistered ⇒ empty) →
  `register_empty_table` (declared schema).
- `Err(other)` → transient ⇒ `Retry`.

`main.rs` connects one `FlightSqlClient::connect(&socket)` and threads it into
`TransformCtx`. No BUCK change — `FlightSqlClient` already lives in the
`engine-wire` crate the worker depends on.

## Testing (TDD)

1. **Failing test first** — add `inline_input_is_read` to `transform_e2e.rs`:
   land an input with `InlineLimits { inline_byte_limit` large, `flush_byte_threshold: i64::MAX }`
   (rows stay **inline**, never flushed), run a transform that selects from it,
   and assert the committed output has the expected non-empty rows (read back
   over Flight, mirroring the existing happy-path assertion). This fails on
   `main` (empty output) and passes after the fix. A `land_inline` helper mirrors
   the existing cold `land` helper with inverted limits.
2. **Regression** — the existing cold-Parquet happy path, the unknown-input
   Abandon, and the empty-input edge tests must all stay green (the empty-input
   test now exercises the `Validation`-means-empty arm).
3. Full `buck2 test //src/services/worker/...` (fixture e2e boots hermetic
   Postgres + engine locally).

## Deferred / follow-ups

- **`#iss-serving-empty-table-not-found` (new, open):** the serving catalog
  returns no provider for a live zero-row table (`serving.rs:176`), so
  `SELECT *`/preview of a legitimately-empty table answers `table not found`
  instead of an empty result. Surfaced as the empty-`employee_floor` preview in
  the reproduction. Out of scope here (this PR makes the transform *produce*
  rows, so the user-visible case disappears); the residual empty-table read
  semantics are a separate, broader serving change. The worker's
  `Validation`-means-empty handling is the local guard until it lands.
- **Streaming input scans** remain the recorded `#fut-transform-followups`
  optimization: inputs are still buffered in worker memory (unchanged from the
  file path). The merged read returns the same materialized batches, so the
  memory profile is unchanged.
- **Cross-input snapshot consistency:** each input is read at its own current
  snapshot (as before — `list_files` also pinned per-input). A stricter
  as-of-consistent multi-input read (via `execute_as_of` at one resolved
  snapshot) is a possible later hardening, not required for correctness parity
  with the prior path.
