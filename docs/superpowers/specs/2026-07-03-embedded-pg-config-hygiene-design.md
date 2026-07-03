# Embedded-Postgres configuration hygiene

**Date:** 2026-07-03
**Status:** approved
**Resolves:** `#iss-embedded-config-requires-pg-bin-dir`, `#iss-embedded-pg-libxml2`,
and folds in `#fut-embedded-pg-db-vars-optional` (that FUTURE entry is removed by
this planning PR; its scope lands here rather than as a standalone item).

## Problem

Three configuration gaps make embedded mode (`LOOM_PG_MODE=embedded`) noisier and
more fragile than it should be:

1. **PG-binary vars demanded by processes that never spawn Postgres.**
   `EmbeddedSettings::from_map` (`src/services/runtime/src/lib.rs`) `req_var`s
   `LOOM_PG_BIN_DIR` unconditionally in embedded mode, but `loom create-admin`
   (`src/services/standalone/src/main.rs::create_admin_cli`) only *connects* via
   `build_pool` — it never boots a cluster. `tools/dev-up.sh` passes
   `LOOM_PG_BIN_DIR`/`LOOM_PG_LD_LIBRARY_PATH` to `create-admin` purely to satisfy
   config parsing.
2. **Meaningless external `LOOM_DB_*` placeholders in embedded mode.**
   `DbConfig::from_map` requires `LOOM_DB_HOST/PORT/USER/PASSWORD` even though
   `build_pool_managed`'s embedded branch ignores `cfg.db` entirely (it connects
   via `EmbeddedPg::connect_options()` — socket, user `postgres`, no password).
   `dev-up.sh` invents `LOOM_DB_PASSWORD=postgres` etc. just to parse.
3. **Missing `libxml2.so.2` fails late and cryptically.** The embedded dist's
   `postgres`/`initdb` dynamically link `libxml2.so.2`, but the extracted `lib/`
   ships only PG's own libs (`managed_postgres_embed::ExtractedPg` documents
   "NOT libxml2"), and `managed_postgres::pg_command` sets `LD_LIBRARY_PATH` to
   exactly the configured path. On hosts lacking a system `libxml2.so.2` (e.g.
   Arch ships soname `.so.16`), `initdb` dies with a raw loader error surfaced as
   `Initdb(exit status)` — no hint what is missing. `dev-up.sh` carries a symlink
   shim as a workaround.

**Standing direction (libxml2):** loom embeds only its own artifacts; system
libraries are the deployment environment's responsibility. The fix is fail-fast
diagnosis + documentation + image posture — **not** bundling the `.so` into the
embed archive.

## Design

### Config-requirements matrix (mode × role)

| Var group | External server | External client-only / migrate | Embedded server | Embedded client-only (`create-admin`) | Embedded migrate |
|---|---|---|---|---|---|
| `LOOM_DB_HOST/PORT/USER/PASSWORD/NAME` | required | required | **defaulted** (below) | **defaulted** | required (targets external PG; migrate never sets embedded mode) |
| `LOOM_PG_BIN_DIR` / `LOOM_PG_LD_LIBRARY_PATH` | n/a | n/a | required **at spawn site** (self-extract injects when absent) | **not required** | n/a |
| `LOOM_DATA_PATH`, `LOOM_BIND_ADDR`, store/tuning vars | unchanged | unchanged | unchanged | unchanged | unchanged |

**Role is learned structurally, not passed.** `Config::from_map` keeps its
signature; PG-path validation moves from parse time to the one code path that
actually spawns a cluster:

- `EmbeddedSettings` gains `bin: Option<PgBinPaths>` (`bin_dir` + `ld_library_path`),
  parsed when `LOOM_PG_BIN_DIR` is present, `None` otherwise — never `req_var`'d.
- `build_pool_managed`'s embedded branch (the sole `EmbeddedPg::start` caller in
  the runtime) requires `bin` and assembles `managed_postgres::EmbeddedPgConfig`
  there; absence is a `ConfigError` naming `LOOM_PG_BIN_DIR` and saying it is
  needed only to *boot* the embedded cluster. `managed_postgres` itself is
  untouched by this half.
- Client-only (`create-admin` → `build_pool`) and migrate (`run_migrations` →
  `cfg.db`) paths never reach the spawn site, so they parse and run without the
  PG-binary vars. The standalone serve path still self-extracts and injects the
  vars before `from_map` when absent, so the server role is unaffected.

### DB-var defaulting in embedded mode (folds `#fut-embedded-pg-db-vars-optional`)

When `LOOM_PG_MODE=embedded`, `DbConfig::from_map` defaults (explicit vars still
override): `LOOM_DB_HOST` → `<LOOM_DATA_PATH>/pgrun` (the socket dir, matching
`EmbeddedSettings`), `LOOM_DB_PORT` → `5432`, `LOOM_DB_USER` → `postgres`,
`LOOM_DB_PASSWORD` → `""` (trust auth over the socket; unused), `LOOM_DB_NAME` →
`loom`. This makes `cfg.db` *consistent by construction* with
`EmbeddedPg::connect_options()`, so client-only tools connect to the same socket
the composite serves — `dev-up.sh` drops its placeholder block. External mode is
byte-for-byte unchanged (all five stay required).

### libxml2: fail-fast + docs + image posture

- **Fail-fast preflight** in `managed_postgres::EmbeddedPg::start`, before
  `initdb`/`spawn_postgres`: run `<bin_dir>/postgres -V` via `pg_command` (cheap,
  side-effect-free). On failure, classify the stderr: a loader error
  (`error while loading shared libraries: <lib>`) becomes a new
  `EmbeddedPgError::MissingSharedLibrary { lib: String }` whose `Display` names
  the missing library, states that loom does not bundle system libraries, and
  points at the host-prerequisites section of `docs/deploy.md`. Non-loader
  failures keep today's error shapes. The stderr→lib classifier is a pure
  function so it is unit-testable without a broken host.
- **Documentation:** `docs/deploy.md` gains a **Host prerequisites** subsection
  under *Single-binary `loom`*: embedded mode needs a system `libxml2` providing
  soname `libxml2.so.2` (package `libxml2` on Wolfi/Debian/Ubuntu/Fedora), with
  the Arch `.so.16` caveat and the `dev-up.sh` shim noted as a dev-only bridge.
- **Image posture:** no standalone `loom` OCI image exists today
  (`deploy/images/` holds engine/ingest/query-api only, all external-PG, none of
  which needs libxml2 — their apko manifests are untouched). The spec mandates:
  when a `deploy//images/loom` standalone image is added, its `apko.yaml` MUST
  include the Wolfi `libxml2` package; this requirement is recorded in the new
  deploy.md subsection so it cannot be lost.

## Acceptance criteria (red-first)

1. **create-admin without PG-binary vars:** a standalone/e2e test invokes
   `loom create-admin` in embedded mode against a running cluster with *no*
   `LOOM_PG_BIN_DIR`/`LOOM_PG_LD_LIBRARY_PATH` in its env; it succeeds. Today it
   fails at parse with `MissingVar("LOOM_PG_BIN_DIR")`.
2. **Spawn-site requirement preserved:** `build_pool_managed` on an embedded
   `Config` whose `bin` is `None` fails with the error naming `LOOM_PG_BIN_DIR`
   (not a panic, not a pathless spawn failure).
3. **Missing libxml2 is named:** (a) the stderr classifier maps
   `error while loading shared libraries: libxml2.so.2: cannot open shared object
   file` to `MissingSharedLibrary { lib: "libxml2.so.2" }`; (b) an
   `EmbeddedPg::start` test pointing `bin_dir` at a stub `postgres` script that
   exits 127 with that stderr yields the named error whose message references
   `docs/deploy.md` — hermetic, no reliance on a host actually lacking the lib.
4. **Embedded mode without `LOOM_DB_*`:** `Config::from_map` with
   `LOOM_PG_MODE=embedded` + `LOOM_DATA_PATH` set and *no* `LOOM_DB_*` vars parses
   with the defaults above (host = `<data>/pgrun`, user `postgres`, db `loom`);
   an explicit `LOOM_DB_NAME` still overrides. External mode without them still
   fails with `MissingVar`.
5. **Composite still green:** existing `composite-e2e`/`composite-error-path`
   suites pass; `tools/dev-up.sh` sheds the placeholder `LOOM_DB_*` and the
   create-admin PG-var workaround (the libxml2 shim stays until hosts catch up).

## Out of scope

- Bundling `libxml2.so.2` into the embed archive, statically linking it, or
  rebuilding Postgres `--without-libxml` (contradicts the standing direction).
- Publishing the standalone `loom` OCI image itself (only the apko requirement
  for whenever it lands).
- A CI self-extract smoke test that exercises a genuinely libxml2-less host
  (needs a bespoke minimal image; the stub-script test covers the error path).
- `LOOM_BIND_ADDR` still being required-but-ignored by the composite, and
  `LOOM_DATA_PATH` under an `s3://` warehouse (`#fut-deploy-data-path-optional-s3`).
- Extraction-cache GC and `pg_upgrade` (`#fut-embedded-pg-cache-gc`,
  `#fut-embedded-postgres-pg-upgrade`).
