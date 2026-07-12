# Guarded operator compact endpoint Design

> **Status:** design (direction). This spec makes `iss-compact-endpoint-unguarded`
> build-ready. The item stays in ISSUES (it is a defect in shipped code); a
> separate work agent writes the implementation plan from it and builds it.

## Problem

The operator compaction endpoint `POST /tables/{schema}/{table}/compact`
(`ingest/src/http.rs:123`, handler `:142-169`) predates the auto-trigger and
carries none of its safeguards:

- **Non-deduped**: it enqueues via plain `queue().enqueue` (`pg_insert`) —
  repeated POSTs pile up duplicate `compact_table` jobs, and it never dedups
  against a pending auto-trigger job.
- **Unguarded**: it accepts declared stream/CDC tables, changelog tables, and
  **shadow-flagged** tables — where compaction re-projects files at a higher
  `begin_snapshot` and the COW merge-on-read (highest `begin_snapshot` wins,
  `iceberg_inline.rs:757-762`) could resurrect tombstoned rows. It doesn't
  even check the table exists (`ingest/tests/compact_endpoint.rs:28-64`
  enqueues for a never-created table).
- **Authentication-only**: the router-level `protect` (`ingest/src/serve.rs:44`
  → `runtime/src/auth.rs:116-118`) applies `require_auth` only; the handler
  never reads the `Subject`. Any authenticated caller can enqueue maintenance
  work — contrast `/admin/schedules`, which is `require_admin`-gated
  (`runtime/src/admin.rs:1794`).

`#road-compaction-auto-trigger` (PR #419) built the shared guard helper +
dedup this endpoint should route through (`maybe_enqueue_compact`,
`postgres/src/iceberg_compact.rs:43-86`) and its close recorded this defect.

## Decision record

**Operator decisions, 2026-07-12 (committed):**

- **Admin-gated.** The compact route gains `require_admin`, matching the
  `/admin/schedules` posture. Maintenance actions are operator surface; the
  guard+dedup alone would leave a cost lever open to any authenticated caller.
- **Operator = eager policy, same correctness.** The endpoint calls
  `maybe_enqueue_compact` with `min_small_files: 2` (the worker's own
  convergence floor, `worker/src/compact.rs:40-42`) instead of the
  auto-trigger's `LOOM_COMPACT_TRIGGER_FILES` — an explicit operator request
  compacts any compactable pair without waiting for threshold N. The
  **correctness guards** (stream/changelog/shadow) still refuse; there is no
  force flag, because those guards prevent row resurrection, not policy.
- **Honest responses**: 404 unknown table; 202 `{job_id}` enqueued; 200
  `{job_id: null}` suppressed — no more unconditional 202.

## Context — what ships today (verified)

- `maybe_enqueue_compact(conn: &mut PgConnection, table: &TableRef, cfg:
  &CompactTriggerCfg) -> Result<Option<JobId>>`
  (`iceberg_compact.rs:43-86`): resolves live `table_id` (`None` for a
  never-written table), one combined SQL guard (declared stream tables via
  `stream.stream_table.table_id`, changelog tables via
  `.changelog_table_id`, shadow via `iceberg_mirror.shadow_flag`), counts
  live small files under `cfg.small_file_bytes`, and enqueues via
  `pg_insert_if_absent` with the same `CompactJob {schema,name}` payload the
  endpoint builds — so operator and auto jobs dedup against each other. Pure
  Postgres, callable outside a commit tx.
- `CompactTriggerCfg { small_file_bytes, min_small_files }`
  (`iceberg_compact.rs:21-30`); ingest already parses both knobs into its
  routing config (`serve.rs:74-81`).
- Ingest's `AppState` (`http.rs:113-117`) holds only
  `materializer` + `cp: Arc<dyn ControlPlane>` — no pool, no cfg. But
  `serve()` has the `PgPool` in scope (`serve.rs:23`; currently moved into
  the materializer) and the parsed routing tuning. `ControlPlane` exposes no
  raw pool, and ingest already depends on `control_plane_postgres` + `sqlx`
  (`http.rs:20`).
- Admin gating machinery: `require_admin` lives in `service_runtime`
  (used by the runtime admin router, `admin.rs:1745-1794`) and can be applied
  per-route via `route_layer`.

## Design

### Threading

Extend ingest's `AppState` with the two missing handles:

```rust
pub struct AppState {
    pub materializer: Arc<dyn LandingMaterializer>,
    pub cp: Arc<dyn ControlPlane>,
    pub pool: sqlx::PgPool,                  // clone; PgPool is an Arc'd handle
    pub compact_small_file_bytes: i64,       // from routing config
}
```

(`serve()` clones the pool before moving it into the materializer.) This
keeps the guard call direct — no new `ControlPlane` trait method, so the
memory adapter is untouched. The endpoint is inherently a
postgres-deployment surface (it enqueues a physical-compaction job), so the
concrete dependency is honest.

### Handler

`compact` becomes:

1. **404** unless `cp.catalog().current_snapshot(&table)` is `Some` (the
   `schedule_table_check` precedent, `admin.rs:1636-1664`).
2. Acquire a pool connection; call `maybe_enqueue_compact(&mut conn, &table,
   &CompactTriggerCfg { small_file_bytes: st.compact_small_file_bytes,
   min_small_files: 2 })`.
3. `Some(job_id)` → **202** `{"job_id": id}` (unchanged happy-path shape).
   `None` → **200** `{"job_id": null}` — suppressed: ineligible
   (stream/changelog/shadow) or nothing-to-do (<2 small files) or deduped
   (a `compact_table` job for this table is already pending). The body
   distinguishes nothing further in v1 — the reasons are deliberately not
   an oracle, and the guard returns no reason granularity.

### Auth

The compact route moves behind `require_admin` via a per-route
`route_layer` in ingest's router (the rest of ingest stays `require_auth` —
landing is a data-plane surface, not operator maintenance). The endpoint's
OpenAPI annotation documents the admin requirement.

### Response-contract note

The always-202 contract changes (202/200/404 split + admin gate). The only
in-tree caller is the e2e/operator surface; no UI or service calls this
endpoint today. Document the change in `docs/system-capabilities/` at close.

## Non-regression

- The auto-trigger, worker, `CompactJob`, and `maybe_enqueue_compact` are
  unchanged — the endpoint becomes a fourth caller of the existing helper.
- `min_small_files: 2` cannot under-run the worker: 2 is the worker's own
  no-op floor.
- No migration, no new SQL (the helper's queries are already prepared).

## Testing

Extend `ingest/tests/compact_endpoint.rs` (fixture test; drive the router
with the admin layer where needed):

- **Dedup** — two POSTs, one available job; POST after an auto-trigger
  enqueue adds nothing.
- **Eligibility refusals** — a declared stream table, its changelog table,
  and a shadow-flagged table each return 200 `{job_id: null}` with zero jobs.
- **Eager policy** — a live table with exactly 2 small files (below the
  auto-trigger's default 8) returns 202 + one job.
- **404** — never-written table.
- **Admin gate** — non-admin authenticated subject → 403; admin → 202.
- **Existing test** `compact_endpoint_enqueues_job` updated for the new
  contract (create the table first).

## Global constraints (loom-specific, carry into the plan)

- Strict clippy (pedantic + restriction); `#[expect(lint, reason)]` locally.
- `buck2 run //tools:prek -- run --all-files` before every commit.
- Tests are `rust_test`/`loom_fixture_test` targets only.
- If any new compile-time SQL appears, `tools/sqlx-prepare.sh` + commit
  `.sqlx` (none is expected).

## Out of scope (deferred)

- **Suppression-reason granularity** in the response body (needs the guard to
  return a reason enum; add if operators ask).
- **Stream/changelog small-file compaction** — `#fut-stream-smallfile-compaction`.
- **Per-table trigger overrides** — `#fut-compact-trigger-pertable-override`.

## Acceptance

1. Repeated POSTs yield exactly one pending job; operator and auto-trigger
   jobs dedup against each other.
2. Stream/changelog/shadow tables are refused (200, no job); unknown tables
   404; non-admin callers 403.
3. A 2-small-file table compacts on operator request without meeting the
   auto-trigger threshold.
4. Existing suites green (`buck2 test //src/...`).

## Interfaces (names the plan consumes)

- Consumes: `maybe_enqueue_compact` + `CompactTriggerCfg`
  (`postgres/src/iceberg_compact.rs:21-86`); `current_snapshot`
  (`core/src/catalog.rs`); `require_admin` (`service_runtime`); ingest
  routing config's `compact_small_file_bytes` (`ingest/src/serve.rs:74-81`).
- Produces: extended `AppState { pool, compact_small_file_bytes, .. }`
  (`ingest/src/http.rs:113`); the rewritten `compact` handler
  (`http.rs:142`); the admin `route_layer` on the compact route
  (`http.rs:123` / `serve.rs:44`).
