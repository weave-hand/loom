# Stream engine — symmetric log-vs-CDC declaration guard Design

> **Status:** design (direction). This spec gives `iss-stream-log-vs-cdc-declare`
> its own fix spec (the defect was recorded by the slice-2 parent
> `2026-07-07-stream-pk-cdc-tables-design` as a deliberate, register-tracked
> asymmetry). The matching implementation plan is
> `docs/superpowers/plans/2026-07-09-stream-log-vs-cdc-declare.md`.

## Goal

Make stream-mode declaration **kind-symmetric**: a `mode=stream&buckets=N` (log)
request against a table already declared `kind='cdc'` must be rejected with
`Validation` (HTTP 400), exactly as the reverse (`mode=cdc` against a log table)
already is — without perturbing the pure log/batch declaration paths that
slice 2's byte-identical constraint pins.

## Context — what ships today, and the defect

### The single declaration seam (already exists)

Both HTTP declaration surfaces bottom out in ONE function. `POST
/datasets/{schema}/{table}?mode=stream&buckets=N` (`land`,
`src/services/ingest/src/http.rs:442`, parse at `:452-460`) threads
`stream_buckets: Some(n)`; `POST /models/{type}?mode=cdc&buckets=N`
(`land_model`, `http.rs:343-373`) threads a `CdcDecl`
(`iceberg_landing.rs:45`). Both reach
`land`/`land_cdc` (`src/control-plane/postgres/src/iceberg_landing.rs:108`/`:148`),
where `combine_stream_decl` (`iceberg_landing.rs:70`) normalizes the pair into
the internal `StreamDecl` enum (`src/control-plane/postgres/src/stream.rs:17`:
`None` | `Log(n)` | `Cdc { buckets, bucket_key, merge_engine }`), and both
landing routes (inline: `iceberg_inline.rs:557`; direct Parquet:
`iceberg_landing.rs:942`) call **`reconcile_stream_mode`** (`stream.rs:55`) on
the write transaction. Any `Conflict`/`Validation` it raises surfaces as a 400
with the message echoed (`IngestError::into_api`, `ingest/src/http.rs:102-104`).

### `reconcile_stream_mode`'s arms, and the hole

`reconcile_stream_mode` matches `(requested, existing)` bucket counts
(`stream.rs:128`):

- `(Some(n), Some(m)) if n != m` → `Conflict` (count mismatch, `stream.rs:129-134`).
- `(Some(_), Some(m))` — the **count-equal redeclare arm** (`stream.rs:135-176`).
  It carries a kind guard, but that guard is **gated on
  `matches!(decl, StreamDecl::Cdc { .. })`** (`stream.rs:141-151`): a `Cdc`
  request against an existing table whose `StreamMeta.kind != StreamKind::Cdc`
  → `Validation` ("cannot declare … as a cdc table: already declared with a
  different stream kind"). The engine-immutability guard (`stream.rs:156-174`)
  is likewise `Cdc`-gated. **A `StreamDecl::Log(n)` request checks nothing but
  the count** and falls through to the accepting `Some(m)` at `stream.rs:175`.
- `(Some(n), None)` → first declare (`stream.rs:177-230`); `(None, existing)` →
  pass-through (`stream.rs:231`).

So a `mode=stream&buckets=N` write whose `N` equals an already-declared **CDC**
table's `bucket_count` is silently **accepted**. The registry row stays
`kind='cdc'` (`pg_declare_stream`'s `ON CONFLICT DO NOTHING` is never even
reached — the count-equal arm declares nothing), and the write proceeds as a de
facto CDC append: the bucketing code reads `pg_stream_meta` and, seeing
`kind == StreamKind::Cdc`, **hash-buckets on the identity `bucket_key`**
(`iceberg_inline.rs:576-600`), not the log path's `row % bucket_count`
(`iceberg_inline.rs:600`). The client asked for a log table and got, with a 200,
rows stamped into someone else's CDC stream (dual-written to its changelog at
flush). A secondary sharp edge: if the dataset-path batch happens not to carry
the CDC table's `bucket_key` column, the accepted write then fails deep in
bucketing with an opaque `Backend` 500 ("cdc bucket_key … not in appended
columns", `iceberg_inline.rs:587-591`) instead of a clean 400 at declaration.

### Reachability

Obscure but real: CDC is declarable only via `POST /models/{type}?mode=cdc`
(the type binds to `main.<type>`'s table), while `mode=stream` arrives via
`POST /datasets/{schema}/{table}` — the two surfaces must target the same
physical table (e.g. declare CDC on type `widget`, then
`POST /datasets/main/widget?mode=stream&buckets=2`). Reserved-prefix or
cross-surface writes are not otherwise blocked; nothing prevents this today.

### Why the asymmetry existed

Slice 2a/2b carried a hard constraint: "**Non-CDC paths must stay
byte-identical** — batch tables, slice-1 log tables … must behave exactly as
before this plan; every branch is gated on the registry recording `kind='cdc'`"
(`docs/superpowers/plans/2026-07-07-stream-pk-cdc-tables-2a.md:18`). The CDC
kind guard's own comment records the consequence: "A `Log` request is
unaffected: it keeps its original count-only comparison, so log/batch behavior
is unchanged" (`stream.rs:138-140`). The register entry notes the fix was
deferred rather than risk perturbing the pinned Log arm mid-slice.

## Architecture — the fix

**A symmetric, kind-aware reject in the count-equal redeclare arm, gated on the
existing registry row being CDC.** In `reconcile_stream_mode`'s
`(Some(_), Some(m))` arm, immediately after the existing `Cdc`-side kind guard
(`stream.rs:141-151`) and before the engine-immutability guard, add:

```rust
// Symmetric kind guard for the Log side (iss-stream-log-vs-cdc-declare):
// a `mode=stream` (log) request against an already-declared table must
// also match its KIND — a cdc table with the same bucket count is not a
// valid log-declare target. Fires only when a `kind='cdc'` registry row
// already exists, so the pure log/batch paths (no such row) stay
// byte-identical.
if matches!(decl, StreamDecl::Log(_))
    && existing_meta
        .as_ref()
        .is_some_and(|meta| meta.kind != StreamKind::Log)
{
    return Err(ControlPlaneError::Validation(format!(
        "cannot declare {}.{} as a log stream table: already declared \
         with a different stream kind",
        table.schema, table.name
    )));
}
```

Properties:

- **No new I/O, no SQL change.** `existing_meta` is already fetched once at
  `stream.rs:124` (`pg_stream_meta`); the guard is a pure in-memory check.
  `StreamKind` is already imported (`stream.rs:3`). No `query!` touched → no
  `tools/sqlx-prepare.sh` / `.sqlx` churn.
- **Both landing routes covered at once.** The guard sits in the shared seam,
  so the inline path (`iceberg_inline.rs:557`) and the direct-Parquet path
  (`iceberg_landing.rs:942`) — and any future internal `land(…,
  stream_buckets)` caller — reject identically, before any declare or row
  write, on the caller's transaction (nothing commits).
- **Error shape mirrors the existing guard exactly**: `Validation` with the
  same "already declared with a different stream kind" sentence (the CDC side's
  message at `stream.rs:146-150`), mapped to a 400 with the message echoed by
  `IngestError::into_api` (`ingest/src/http.rs:102-104`).
- **Count-mismatch precedence is preserved.** A log request against a CDC table
  with a *different* count keeps today's `Conflict` (count mismatch) from the
  `n != m` arm — the same precedence the CDC-side guard already has (a
  `mode=cdc` against a log table with a different count also Conflicts on count
  first). Both mismatches reject; only the diagnostic differs. Reordering the
  checks would change pinned behavior for zero benefit.
- **Comment/doc hygiene**: the now-false parenthetical at `stream.rs:138-140`
  ("A `Log` request is unaffected …") is rewritten to describe the symmetric
  pair, and `reconcile_stream_mode`'s doc comment (`stream.rs:37-46`) gains the
  log-vs-cdc rejection alongside the listed rejections.

### Rejected alternative — "a single normalized declaration seam"

The register's second fix shape ("a single normalized declaration seam both
endpoints share") turns out to already be the shipped architecture:
`combine_stream_decl` → `StreamDecl` → `reconcile_stream_mode` **is** that
seam, and both HTTP surfaces already flow through it. The defect is a missing
guard *inside* the seam, not a missing seam — so the alternative reduces to
this fix. A further unification (deriving a `requested_kind` from `StreamDecl`
and comparing kinds once for both directions) was considered and rejected: the
`StreamDecl::None` arm would need an unreachable branch (clippy's
restriction-group panic lints forbid `unreachable!` in prod code, and a
`Result`-returning helper is more machinery than two parallel three-line
guards), and rewriting the existing CDC guard risks perturbing tested behavior
for a pure-cosmetics win. Two adjacent, symmetric guards keep the diff minimal
and the existing guard byte-identical.

## Non-regression — the byte-identical log/batch constraint

The guard fires **only** when a `kind='cdc'` registry row pre-exists — by
definition not a pure log/batch path. Every pinned path is unchanged:

- **Pure log/batch declares** — `//src/services/ingest:stream-declare`
  (`src/services/ingest/tests/stream_declare.rs`) pins all four: first write
  declares with the requested count; count mismatch → 400; batch→stream
  conversion → 400; plain write leaves no `stream.stream_table` row. None
  involve a CDC row, so the new guard is dead code for all of them.
- **Log same-count redeclare stays accepted** — currently untested directly;
  this spec's seam test adds the control case (a second `mode=stream` write
  with the matching count on a *log* table still lands).
- **CDC declares** — `//src/services/ingest:model-cdc-declare` (declare, no
  identity → 400, batch→cdc → 400), `//src/services/query-api:stream-cdc-declare`
  (first-declare creates the changelog + registry pointer), and
  `//src/control-plane/postgres:stream-merge-declare` (Versioned validation +
  engine-immutability `Conflict` — exercises the very count-equal arm the new
  guard is inserted into, with `Cdc` decls that must not trip it).
- **Non-declaring writes** — `StreamDecl::None` requests (every plain append,
  including appends to CDC tables) take the `(None, existing)` arm and never
  reach the guard; the slice-2 emission/flush suites
  (`stream_cdc_emission`, `stream_cdc_bucket`, `stream_flush_persist`,
  `stream_bucket_check`, query-api `stream_cdc_e2e`/`stream_cdc_consolidate`)
  stay green and byte-identical.

## Testing

Tests are `rust_test` / `loom_fixture_test` integration targets only — never
inline `#[cfg(test)]`. Both new tests use `loom_fixture_test` and mirror named
siblings.

- **Seam test** (`src/control-plane/postgres/tests/stream_log_vs_cdc_declare.rs`,
  mirroring `stream_merge_declare.rs`'s `PgFixture`/`local_sql_catalog`/
  `land_cdc` harness): (1) CDC-declare a table via `land_cdc`, then a
  `land(…, stream_buckets: Some(N))` with the **matching** count →
  `Err(Validation)` and the registry still reads `kind='cdc'` — the defect's
  exact repro, red before the fix; (2) mismatched count → `Err(Conflict)`
  (pins the preserved precedence); (3) control: log-declare a fresh table, then
  a second same-count `mode=stream` write → `Ok` (pins the untouched log
  redeclare).
- **Cross-surface HTTP e2e** (`src/services/ingest/tests/stream_log_vs_cdc_http.rs`,
  mirroring `model_cdc_declare.rs`'s auth + raw-SQL readback harness):
  `POST /models/widget?identity=id&mode=cdc&buckets=2` → 200; then
  `POST /datasets/main/widget?mode=stream&buckets=2` → **400** with "different
  stream kind" in the body; `stream.stream_table` readback still
  `(2, 'cdc', Some("id"))`. Proves the obscure two-surface reachability end to
  end.

## Global constraints (loom-specific, carry into the plan)

- **Tests are `rust_test` / `loom_fixture_test` integration targets only.** New
  fixture tests MUST use `loom_fixture_test`
  (`src/control-plane/postgres/defs.bzl`), not a bare `rust_test`. The
  `no-inline-tests` prek hook fails the build on any inline `#[test]`.
- **No `.sqlx` impact.** The fix adds no `query!`/`query_scalar!` SQL (the
  guard reuses the already-fetched `existing_meta`), so `tools/sqlx-prepare.sh`
  is NOT needed. If the implementation drifts into new compile-time SQL,
  either run `sqlx-prepare.sh` locally and commit `.sqlx/`, or (cloud sessions,
  which cannot boot `initdb` as root) use a runtime
  `sqlx::query(AssertSqlSafe(…))` as `stream_declare.rs`'s readback and
  `version_for_table` do.
- **Run prek before every commit:** `buck2 run //tools:prek -- run --all-files`
  (rustfmt is a separate hook; stage new files with `git add` first). Markdown
  files end with exactly one trailing newline, no trailing whitespace.
- **Clippy is strict (pedantic + restriction):** no `unwrap`/`expect`/
  `indexing_slicing`/`panic`/`unreachable`/`todo` in production code; the guard
  uses only `matches!`/`is_some_and`. Test code is exempted from the
  panic-safety lints via `loom_fixture_test`.
- **Non-CDC / log / batch paths must stay byte-identical** (the slice-2
  constraint): the guard must be gated on a pre-existing `kind='cdc'` row;
  `//src/services/ingest:stream-declare` and the slice-2 suites must pass
  unchanged.
- **Build/test commands:** build `buck2 build -v0 --console none //src/...`;
  test `buck2 test --console none <targets>` (cloud: `-M none` on builds,
  scope tests; full local suite needs `-j 8` for the PG boot-slots).

## Non-goals (explicit — deferred or out of scope)

- **Changing count-vs-kind error precedence.** A kind mismatch with a
  *different* count still reports the count `Conflict` first (both directions;
  matches the existing CDC-side behavior). Both reject — only the message
  differs.
- **Unifying the two kind guards into one requested-kind expression** — see the
  rejected alternative; cosmetic, riskier, no behavior gain.
- **Blocking `StreamDecl::None` (plain, modeless) appends to CDC tables via
  `POST /datasets`** — accepted today by the `(None, existing)` arm as `+I`
  events with stamping; whether the dataset surface should require an explicit
  mode for stream tables is a separate product question, not this defect.
- **Log↔CDC conversion / bucket-count changes** — immutable in v1, per the
  slice-2 spec.
- **A clean 400 for a CDC-table append missing the `bucket_key` column**
  (`iceberg_inline.rs:587-591`'s `Backend` 500) — this fix makes the
  known trigger unreachable via `mode=stream`, but the modeless-append edge
  remains; file separately if observed.

## Interfaces

- Consumes:
  - `reconcile_stream_mode` (`src/control-plane/postgres/src/stream.rs:55`) —
    the count-equal arm `stream.rs:135-176`; the CDC kind guard
    `stream.rs:141-151` (placement + message template); `existing_meta` from
    `pg_stream_meta` (`stream.rs:124`, fn at `:367`).
  - `StreamDecl` (`stream.rs:17`), `StreamKind`/`StreamMeta`
    (`control_plane_core`, imported at `stream.rs:3`),
    `ControlPlaneError::Validation`.
  - `land` / `land_cdc` / `combine_stream_decl` / `CdcDecl`
    (`src/control-plane/postgres/src/iceberg_landing.rs:108`/`:148`/`:70`/`:45`)
    — unchanged, exercised by the tests.
  - Ingest HTTP: `land` handler + `StreamParams` parse
    (`src/services/ingest/src/http.rs:442`/`:452-460`), `land_model` CDC parse
    (`http.rs:343`), `IngestError::into_api` 400 mapping (`http.rs:102-104`) —
    all unchanged.
  - Test harnesses: `stream_merge_declare.rs`
    (`src/control-plane/postgres/tests/`, BUCK target `stream-merge-declare`,
    `src/control-plane/postgres/BUCK:1414`) and `model_cdc_declare.rs`
    (`src/services/ingest/tests/`, BUCK target `model-cdc-declare`,
    `src/services/ingest/BUCK:240`).
- Produces (the plan relies on these EXACT names):
  - The `StreamDecl::Log(_)`-gated kind guard in `reconcile_stream_mode`'s
    `(Some(_), Some(m))` arm, raising
    `ControlPlaneError::Validation("cannot declare {schema}.{table} as a log
    stream table: already declared with a different stream kind")`.
  - `//src/control-plane/postgres:stream-log-vs-cdc-declare`
    (`tests/stream_log_vs_cdc_declare.rs`).
  - `//src/services/ingest:stream-log-vs-cdc-http`
    (`tests/stream_log_vs_cdc_http.rs`).
