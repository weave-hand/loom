# Embedded Postgres lifecycle (local single-binary, slice 1)

Status: design. Author: brainstorm session 2026-06-28.

## Why

Make loom **deployable as a single self-contained binary** that runs on a
laptop with no external Postgres server and no external object store. Two
prior findings shape the scope:

- **Object storage is already local-first.** `store-config` selects the backend
  by `LOOM_WAREHOUSE_URI` scheme; `file://` + `LocalFileSystem` is the default,
  S3/MinIO is opt-in. Nothing to build there to "go local".
- **The control plane is the gap.** Every service builds a `PgPool` from
  `service_runtime::Config::from_env()` + `build_pool(&cfg.db)`, which today
  expects an externally-provisioned Postgres reachable by URL.

The decision (brainstorm): keep **one** control-plane backend everywhere and
keep it **Postgres** — make it self-contained by having loom own an embedded
Postgres lifecycle, rather than introduce a second (e.g. SQLite) adapter. This
reuses the entire `control-plane-postgres` adapter, the committed `.sqlx`
cache, the vendored iceberg `SqlCatalog`, advisory locks, and `LISTEN/NOTIFY`
unchanged. The cost is footprint (embedded PG binaries) and a child process —
not an adapter rewrite.

## The 3-slice arc (context)

This spec covers **slice 1 only**. The full single-binary target decomposes as:

1. **Embedded-Postgres lifecycle** (this spec) — boot `initdb`-if-needed →
   `postgres` → unix socket → migrate → `PgPool`, against a **persistent** data
   dir, with clean shutdown. Independently useful: any single service can run
   against a managed cluster. Reuses `fixture.rs`'s ephemeral-cluster logic,
   promoted to persistent runtime.
2. **Embed + self-extract** — compress the PG distribution into the loom binary
   (`include_bytes!`), extract to a content-addressed cache dir at startup, so
   no PG binaries need to exist on disk beforehand. Also adds the embedded
   `sqlx::migrate!` runner (no migrations-on-disk), solved with the same buck
   asset-materialization mechanism. Makes it a true single file.
3. **All-in-one in-process binary** — one process boots the embedded PG once and
   runs engine (tonic/UDS) + ingest (HTTP) + query-api (HTTP) as tasks in one
   tokio runtime against the local warehouse.

Slice 1 is the foundation and de-risks 2 and 3. Slices 2 and 3 get their own
specs.

## Scope (slice 1)

**In:**

- A new crate `src/services/managed-postgres/` exposing an `EmbeddedPg`
  lifecycle type.
- A `service_runtime` seam selecting external vs embedded Postgres.
- Migration-on-start via the **existing** directory-based
  `control_plane_postgres::run_migrations(pool, dir)`. (The true no-disk
  `sqlx::migrate!` embed is **deferred to slice 2** — there is no precedent for
  compile-time file embedding under buck2 in this repo, and slice 1's test
  already receives `LOOM_MIGRATIONS_DIR` from the fixture, so the embed is solved
  alongside the PG-binary embedding with the same extract-to-dir mechanism.)
- A fixture-backed test proving idempotent init, restart, and data persistence.

**Out (later slices / explicitly deferred):**

- Asset embedding / self-extraction of the PG binaries (slice 2). Slice 1 takes
  the PG `bin/` dir as a **configurable path** (point it at the buck-materialized
  `:postgres-bin`, exactly as the fixture does).
- In-process composition of the services (slice 3).
- PG **major-version migration** of an existing data dir (binary bump meets old
  cluster). Out of scope; detect and error clearly, document the manual path.
- Windows support. Unix (Linux/macOS) only, consistent with the rest of loom.

## Design

### `managed-postgres` crate

A standalone crate (not a `service_runtime` module) so its process-management
and file-locking dependencies stay out of the zero-pool transform worker, which
must not pull in a Postgres lifecycle. `service_runtime` depends on it.

```rust
pub struct EmbeddedPgConfig {
    /// Postgres `bin/` directory (holds initdb, postgres, pg_ctl). Slice 1: a
    /// configurable path (e.g. the buck `:postgres-bin` output). Slice 2 fills
    /// this from the extracted asset.
    pub bin_dir: PathBuf,
    /// LD_LIBRARY_PATH for the spawned server (libxml2/ICU), as the fixture's
    /// POSTGRES_LD_LIBRARY_PATH.
    pub ld_library_path: String,
    /// PERSISTENT cluster data dir. Default: `<LOOM_DATA_PATH>/pgdata`.
    pub data_dir: PathBuf,
    /// Runtime dir for the unix socket. Default: `<LOOM_DATA_PATH>/pgrun`.
    pub socket_dir: PathBuf,
    /// Application database name. Default: "loom".
    pub database: String,
}

/// A running, owned embedded Postgres. Dropping it best-effort stops the server;
/// callers should prefer `shutdown()` for a clean `pg_ctl stop -m fast`.
pub struct EmbeddedPg { /* child/pgctl handle, socket path, data_dir, lock guard */ }

impl EmbeddedPg {
    /// Boot (or adopt) a persistent cluster and return a ready handle:
    /// 1. refuse if running as uid 0 (initdb/postgres reject root anyway);
    /// 2. acquire an exclusive file lock on the data dir (single owner);
    /// 3. initdb if `data_dir/PG_VERSION` is absent, else adopt in place;
    /// 4. start `postgres` on a unix socket, listen_addresses='';
    /// 5. wait until it accepts connections (bounded poll-connect retry);
    /// 6. create `database` if absent.
    pub async fn start(cfg: EmbeddedPgConfig) -> Result<EmbeddedPg, EmbeddedPgError>;

    /// Socket-based connect options for the application database (feeds `PgPool`).
    pub fn connect_options(&self) -> PgConnectOptions;

    /// Clean shutdown: `pg_ctl stop -m fast`, release the lock.
    pub async fn shutdown(self) -> Result<(), EmbeddedPgError>;
}
```

### Lifecycle behaviours

- **Root refusal (early, explicit).** `start` checks `geteuid() == 0` first and
  returns `EmbeddedPgError::RunningAsRoot` with a clear message ("loom's embedded
  Postgres cannot run as root — run as a normal user"). This pre-empts the
  cryptic downstream `initdb` failure. Aligns with the deploy's existing non-root
  posture (uid 65532) and with `loom_fixture_test`'s reason for local routing.
- **Idempotent init.** Presence of `data_dir/PG_VERSION` ⇒ adopt the existing
  cluster (no `initdb`). Absent ⇒ `initdb -D data_dir -U postgres --auth=trust`.
  This is the fixture's logic with a persistent dir instead of a tempdir.
- **Single-owner guard.** An advisory file lock (`flock`) on
  `data_dir/loom-embedded.lock` held for the handle's life. A second loom on the
  same data dir fails fast (`EmbeddedPgError::AlreadyLocked`) rather than two
  postmasters fighting over one cluster. (Postgres' own `postmaster.pid` is a
  backstop; the file lock gives a loom-level error before `initdb`/start.)
- **Start + wait-ready.** Spawn `postgres` with `-k socket_dir -c
  listen_addresses=''` (socket-only; no TCP port exposed). Readiness is a bounded
  retry loop opening a connection on the socket (the fixture already does this) —
  not a fixed sleep.
- **Clean shutdown.** `shutdown()` runs `pg_ctl stop -m fast`. `Drop` does a
  best-effort stop so a panic doesn't leak a postmaster. Slice 3 wires
  `shutdown()` to SIGINT/SIGTERM; slice 1 exercises it directly in the test.
- **Crash recovery.** A stale `postmaster.pid` from a previous hard kill is left
  for Postgres to resolve on start (it does this natively); the file lock is
  advisory and released on process exit, so a crashed loom does not wedge the
  data dir.

### Migrations

**Slice 1** reuses the **existing** `control_plane_postgres::run_migrations(pool,
migrations_dir)` (a `sqlx::migrate::Migrator::new(dir)`, idempotent via
`_sqlx_migrations`). `EmbeddedPg::start` does **not** migrate by itself —
migration is `service_runtime::build_pool_managed`'s job after it has the pool
(keeps the lifecycle crate free of the control-plane dep). The migrations are
plain SQL (verified: no `psql` meta-commands, no manual `BEGIN/COMMIT`, no
extensions), so the runner applies them as-is and re-runs as a no-op.

**Slice 2** adds the true no-disk variant — an embedded `sqlx::migrate!("migrations")`
runner that bakes the SQL into the binary at compile time — so the self-contained
binary needs no migrations on disk. This is deferred because compile-time file
embedding under buck2 is untrodden in this repo (no existing `include_str!` /
`include_bytes!` / `sqlx::migrate!`), so its buck materialization is solved
together with the PG-binary embedding (same mechanism), not on slice 1's path.

### `service_runtime` seam

`DbConfig` gains a mode, selected by env, leaving external-PG the default:

```rust
pub enum DbBackend {
    External(PgConnectOptions),  // today's behaviour, from LOOM_DATABASE_* / URL
    Embedded(EmbeddedPgConfig),  // LOOM_PG_MODE=embedded
}
```

- `Config::from_env`: `LOOM_PG_MODE=embedded` ⇒ build an `EmbeddedPgConfig` with
  `data_dir`/`socket_dir` derived from `LOOM_DATA_PATH` and `bin_dir`/
  `ld_library_path` from `LOOM_PG_BIN_DIR`/`LOOM_PG_LD_LIBRARY_PATH` (slice 2
  replaces those two with the extracted-asset path). Anything else ⇒ `External`,
  unchanged.
- A new `build_pool_managed(&cfg) -> Result<(PgPool, Option<EmbeddedPg>)>`:
  - `External` ⇒ `(build_pool(...)?, None)` — identical to today.
  - `Embedded` ⇒ `EmbeddedPg::start(...)`, build a `PgPool` from
    `connect_options()`, run `run_embedded_migrations(&pool)`, return
    `(pool, Some(handle))`.
- The caller **owns** the returned `Option<EmbeddedPg>` for the process lifetime
  and calls `shutdown()` on exit. Existing service `main`s that only need a pool
  keep calling `build_pool`; the embedded path is opt-in via the new function
  (slice 3's all-in-one binary is its first consumer).

### Data layout (persistent)

Under `LOOM_DATA_PATH` (already loom's local data root):

```
$LOOM_DATA_PATH/
  warehouse/        # iceberg file:// warehouse (existing)
  pgdata/           # persistent Postgres cluster (new)
  pgrun/            # unix socket dir (new)
```

The cluster survives restarts; `start` adopts `pgdata` in place. This is the
intended default; the dirs are configurable for tests.

## Testing

A single `loom_fixture_test` (local-routed, because `initdb` refuses root on RE —
same reason as every other fixture test) proves the slice's contract. It points
`bin_dir`/`ld_library_path` at the buck `:postgres-bin` + `:libxml2` outputs (no
asset embedding yet) and uses a **temp dir as the persistent data dir** within
the test:

`tests/embedded_lifecycle.rs`:

1. `EmbeddedPg::start` on a fresh dir → assert `initdb` ran (`PG_VERSION` now
   exists) and the `loom` db exists. Then build a pool and call
   `run_embedded_migrations(&pool)` → assert it applies the full set and the
   `loom` schema is present. (This mirrors the managed `build_pool_managed`
   sequence: start, then migrate.)
2. Write a sentinel row (e.g. enqueue a job, or insert into a migrated table),
   then `shutdown()`.
3. `EmbeddedPg::start` again on the **same** dir → assert (a) no re-`initdb`
   (detectably: `PG_VERSION` mtime/inode unchanged, or an init-counter); then
   `run_embedded_migrations(&pool)` again → assert (b) it applies **zero** new
   migrations (`_sqlx_migrations` count unchanged) and (c) the sentinel row is
   still present.
4. `shutdown()` cleanly.

That single test exercises idempotent init + restart + persistence + clean
shutdown — the whole point of slice 1. A second small test asserts the root
refusal path is reachable as a typed error (skipped when not root; it is a
guard-rail check, not a privileged test).

The crate's pure-logic surface (config parsing, path derivation, error mapping)
gets ordinary `rust_test` unit targets (RE-eligible) per the no-inline-tests
rule.

## Risks / open questions

- **PG version skew on an existing data dir.** A future `:postgres-bin` major
  bump meeting an old `pgdata` will fail to start. Slice 1 detects the
  `PG_VERSION` mismatch and errors clearly (no silent data loss); an
  upgrade/`pg_upgrade` story is deferred (note in FUTURE).
- **macOS vs Linux.** `initdb`/`postgres`/`pg_ctl` invocation is identical; the
  `:postgres-bin` per-arch selection already exists. The socket-dir length limit
  (~104 bytes on macOS) constrains `LOOM_DATA_PATH` depth — document it.
- **Startup latency.** First boot pays `initdb` (~1-2s) once; subsequent boots
  adopt in place and only pay server start + readiness. Acceptable for a local
  binary.
- **Footprint.** Tracked in slice 2 (embedding). Slice 1 adds no binary size —
  it consumes an external `bin/` path.

## Register note

On completion, add a ROADMAP item (`area:` deploy/runtime) for the
single-binary arc, with this spec as `spec:` and slices 2/3 as `[[links]]`. The
PG-version-upgrade gap goes to FUTURE.
