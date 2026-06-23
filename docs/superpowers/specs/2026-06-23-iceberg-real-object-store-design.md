# Real object store (S3/MinIO) for Iceberg

_Design spec. 2026-06-23._

## Context

loom's Iceberg adapter has only ever been exercised against local `file://`
storage. Every service boots the catalog with a hardcoded `LocalFsStorageFactory`
and a `format!("file://{}", data_path)` warehouse URI; the test fixtures use
tempdirs. The vendored SQL catalog and the `iceberg` 0.9 `FileIO` it builds are
already storage-agnostic — paths flow through as opaque URLs and `FileIO` can
resolve `s3://` once given an S3-capable `StorageFactory` — but loom has never
wired or tested that path (`[[fut-iceberg-real-object-store]]`).

This slice makes the Iceberg backend usable against S3-compatible object storage
and, crucially, **proves it** with a hermetic MinIO round-trip. It is
production-readiness for the Iceberg engine, adjacent to (but not gating) the
Iceberg-default flip (`[[fut-replace-ducklake-decision]]`): real object storage
is a precondition for running Iceberg anywhere but a single local disk.

**Scope is Iceberg only.** DuckLake's `Arc<dyn ObjectStore>` seam
(`service_runtime::local_store`) stays `file://` in this slice even though both
share `service_runtime` — routing DuckLake through the same S3 config is a
separate, additive follow-up and would double the backends-under-test here.

## Current state

The storage seam is small and already abstracted; the gaps are all "config never
read, S3 path never built, S3 never tested."

- **Catalog construction (hardcoded, ×3).** Each service builds the Iceberg
  catalog with `LocalFsStorageFactory` + a `file://` warehouse:
  - `src/services/ingest/src/main.rs` (~`:76`,`:79`)
  - `src/services/engine/src/main.rs` (~`:28`,`:31`)
  - `src/services/query-api/src/main.rs` (~`:94`,`:97`)
  Each calls `SqlCatalogBuilder::default().with_storage_factory(Arc::new(LocalFsStorageFactory))`
  `.warehouse_location(format!("file://{}", cfg.data_path.display()))`.
- **Factory → FileIO** (`src/control-plane/postgres/src/iceberg_sql_catalog/catalog.rs`):
  `SqlCatalogBuilder` takes `with_storage_factory(Arc<dyn StorageFactory>)` (`:135`);
  `SqlCatalog::new` builds `FileIOBuilder::new(factory).build()` (`:233`) and stores
  the `FileIO` (`:199`), used for all metadata reads (`:946`). The `StorageFactory`
  is the single injection point that determines which URI schemes resolve.
- **Paths are already URL-opaque.** `iceberg_writer.rs:32` stores
  `df.file_path().to_string()` verbatim; `serving_datafusion.rs:87` consumes
  absolute URLs from the mirror; `iceberg_mirror.rs` stores paths as given. The
  only `file://`-specific code is a **test-only** `strip_prefix("file://")`
  (`tests/iceberg_writer.rs:93`).
- **Config plumbing** (`src/services/runtime/src/lib.rs`): `Config::from_env`
  reads `LOOM_DATA_PATH` (mandatory) and DB/bind vars (`:94`–`:137`). There is
  **no** warehouse-URI or object-store-credential env. Backend *selection* already
  follows an env→enum pattern (`LOOM_LANDING_BACKEND`/`LOOM_SERVING_BACKEND`) we
  mirror.
- **Test fixtures** (`src/control-plane/postgres/src/fixture.rs:580`–`:660`):
  `IcebergWriter` builds a tempdir warehouse + `LocalFsStorageFactory`; the
  `iceberg_*` tests (`tests/iceberg_writer.rs`, `iceberg_landing.rs`,
  `iceberg_flush.rs`) all go through it. Hermetic external services (Postgres,
  DuckDB) are booted from pinned vendored binaries via `loom_fixture_test`
  (`src/control-plane/postgres/defs.bzl`) — the pattern a MinIO fixture follows.

## Decision

Make the Iceberg `StorageFactory` and warehouse URI **env-driven, scheme-selected**,
and prove the S3 path with a **hermetic MinIO** integration test. Existing
deploys that set no new env see byte-identical behavior.

### 1. Config surface (`service_runtime`)

Extend `service_runtime::Config` with an `ObjectStoreConfig` parsed in
`from_env`:

- `LOOM_WAREHOUSE_URI` — optional base warehouse URI. **Unset ⇒
  `file://{LOOM_DATA_PATH}`** (back-compat; existing behavior unchanged). The URI
  **scheme selects the backend**: `file://…` → local, `s3://bucket/prefix` → S3.
- S3/MinIO credentials via **standard AWS env**, so the same path drives real AWS
  and MinIO:
  - `AWS_ENDPOINT_URL` — custom endpoint (set for MinIO; empty for real AWS).
  - `AWS_REGION` — region (default `us-east-1` when an endpoint is set).
  - `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` — credentials.
  - **Path-style access** is implied whenever `AWS_ENDPOINT_URL` is set (MinIO
    requires it); virtual-hosted style otherwise.

Validation: an `s3://` warehouse with missing credentials is a startup error
(fail fast at boot, not at first write).

### 2. Factory seam

Add `service_runtime::build_storage_factory(&ObjectStoreConfig)
-> Result<Arc<dyn StorageFactory>>`:

- `file://` → `LocalFsStorageFactory` (today's behavior).
- `s3://` → an **`S3StorageFactory`** that builds the `iceberg` `FileIO` with S3
  properties (`s3.endpoint`, `s3.region`, `s3.access-key-id`,
  `s3.secret-access-key`, `s3.path-style-access`), sourced from the config.

The three service `main.rs` files stop hardcoding the factory and the `file://`
warehouse: they call `build_storage_factory(&cfg.object_store)?` and pass
`cfg.object_store.warehouse_uri` to `.warehouse_location(...)`. **No change** to
`iceberg_writer`, `iceberg_mirror`, the serving path, or the catalog's read/write
logic — they already handle whatever URL `FileIO` returns.

**Open implementation question (for the plan, not blocking direction):** whether
the vendored SQL catalog / `iceberg` 0.9 already exposes an S3 storage factory we
can parameterize, or we add a thin `S3StorageFactory` alongside the vendored
`LocalFsStorageFactory` (feeding the same S3 `FileIO` props). Either way the
`service_runtime` seam and the env contract above are unchanged; the plan picks
the concrete factory.

### 3. The proof — hermetic MinIO `loom_fixture_test`

The point of this item is that S3 has never been exercised, so the deliverable is
a test that drives a **real S3 server**:

- **Vendor a pinned MinIO server binary** in `tools/BUCK` (mirroring
  `:postgres-bin`/`:duckdb-cli`: per-arch download, pinned version + sha256,
  exposed as a buck target). MinIO ships static per-arch server binaries.
- **MinIO fixture** (mirroring the Postgres fixture): pick a free port, launch
  `minio server` against a tempdir with known root creds, wait until ready, and
  **create the warehouse bucket programmatically** via a minimal S3 `PUT bucket`
  call (using the `object_store`/HTTP deps already in-tree) — deliberately *no*
  second vendored `mc` binary. Tear the server down on drop.
- **Round-trip test** (e.g. `tests/iceberg_s3_roundtrip.rs`, a `loom_fixture_test`
  so it routes local and never RE — MinIO, like `initdb`, won't run as root on
  RE): boot MinIO, build the Iceberg catalog through `build_storage_factory` with
  an `s3://bucket/warehouse` URI + the fixture's endpoint/creds, **land/append an
  Iceberg table**, read it back through the serving engine, and assert both: rows
  match, **and** the mirror's `data_file` paths are `s3://…` (data physically
  lives in MinIO, not on local disk). This closes the "never exercised" gap with a
  real object-store write+read.

## Out of scope (deferred)

- **DuckLake object store** — iceberg only; DuckLake's `ObjectStore` seam stays
  `file://`. Routing it through the same config is an additive follow-up.
- **Real AWS S3 in CI** — MinIO is the hermetic oracle; real-AWS runs use the same
  code path but aren't automated here (`[[fut-iceberg-real-object-store]]` is
  closed by the MinIO proof; live-AWS validation is operational, not a test gate).
- **Helm/deploy S3 wiring** — surfacing the new env in the chart is a
  `[[fut-deploy-followups]]`-class follow-up, not this slice.
- **Advanced credentials** — env vars only; no IAM role/STS/instance-profile/
  web-identity credential chains.
- **Tuning** — multipart thresholds, retry/backoff, and connection pooling use
  the `FileIO`/object-store defaults; no loom-level knobs.

## Affected items

- Promotes `[[fut-iceberg-real-object-store]]` (FUTURE → promoted) to
  `[[road-iceberg-real-object-store]]` (ROADMAP, planned).
- Related: `[[fut-replace-ducklake-decision]]` (object storage is a precondition
  for Iceberg-default in any non-toy deployment, though not one of its three
  named parity gates).
