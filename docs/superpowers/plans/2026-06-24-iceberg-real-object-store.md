# Real Object Store (S3/MinIO) for Iceberg — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make loom's Iceberg backend usable against S3-compatible object storage (env-driven, scheme-selected) and prove it with a hermetic MinIO write+read round-trip.

**Architecture:** iceberg 0.9.1 ships **no** S3 `Storage` backend (only `file://`/`memory://`; `config/s3.rs` is config types only). So we add a self-contained `S3Storage`/`S3StorageFactory` in the postgres crate that implements iceberg's `Storage`/`StorageFactory` traits over `object_store`'s `AmazonS3` (object_store's `aws` feature) — no opendal, no new uninspectable dep tree. `service_runtime` gains an `ObjectStoreConfig` parsed from env (`LOOM_WAREHOUSE_URI` scheme-selects the backend; standard `AWS_*` vars carry creds) plus a `build_storage_factory` seam the three service `main.rs` files call instead of hardcoding `LocalFsStorageFactory`. The DataFusion serving read path — which hardwires `LocalFileSystem` — is made scheme-aware so `s3://` data files read back through the real serving engine. A pinned MinIO server binary is vendored in buck (mirroring `postgres-bin`/`duckdb-cli`); a `MinioFixture` boots it and the round-trip `loom_fixture_test` proves a physical S3 write+read.

**Tech Stack:** Rust 2024, buck2, iceberg 0.9.1 (`Storage`/`StorageFactory`, `#[typetag::serde]`), object_store 0.13.2 (`aws` feature → `AmazonS3`), typetag 0.2, DataFusion 54, reqwest + hmac + sha2 + hex (test-only, AWS SigV4 bucket creation), MinIO server (vendored).

## Global Constraints

- **Tests are `rust_test` integration targets only — never inline `#[cfg(test)]`.** The `no-inline-tests` prek hook fails any first-party `src/**.rs` containing `#[test]`/`#[tokio::test]`. Put unit tests in a sibling `tests/<name>.rs` wired as its own target in the crate's `BUCK`.
- **Fixture tests (boot Postgres/MinIO/DuckDB) MUST use the `loom_fixture_test` macro**, not a bare `rust_test`, or they route to RE and fail as root.
- **Back-compat is mandatory.** With no new env set, `LOOM_WAREHOUSE_URI` defaults to `file://{LOOM_DATA_PATH}` and every service behaves byte-identically to today. No existing test changes behavior.
- **iceberg's `Storage`/`StorageFactory` traits are `#[typetag::serde(tag = "type")]`** — every impl MUST carry `#[typetag::serde]` and be `Serialize + Deserialize`. Hold non-serializable runtime handles (the built `AmazonS3`) behind `#[serde(skip)]` + lazy `OnceLock`, mirroring iceberg's own `FileIO`.
- **After any `Cargo.toml` change, regenerate `third-party/BUCK`** with `./tools/buckify.sh` and commit it (the `reindeer-check` hook enforces no drift). reindeer **unions** features across the workspace, so adding `features = ["aws"]` to object_store in one crate turns it on for the shared `//third-party:object_store` target consumed everywhere.
- **buck2 test stdout must not be piped to `tail`/`head`** — redirect to a file and grep: `buck2 test … > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`.
- **postgres crate pins arrow/parquet 57** (renamed `parquet57`/`arrow-ipc57`/`arrow-select57`); do not introduce bare arrow-58 deps there.
- No SQL changes in this slice ⇒ no `.sqlx` regeneration needed.
- **MinIO, like `initdb`, won't run as root on RE** — the round-trip test must be a `loom_fixture_test` (local-routed).
- **Scope is Iceberg only.** DuckLake's `service_runtime::local_store` stays `file://` (deferred follow-up).

## File Structure

**Create:**
- `src/control-plane/postgres/src/iceberg_sql_catalog/s3_storage.rs` — `S3Storage` (impl `Storage`), `S3StorageFactory` (impl `StorageFactory`), `S3FileRead` (impl `FileRead`), `S3FileWrite` (impl `FileWrite`).
- `src/control-plane/postgres/tests/s3_storage.rs` — pure-logic unit tests (key derivation, serde round-trip, factory→storage) — a plain `rust_test` (no fixture).
- `src/control-plane/postgres/tests/iceberg_s3_roundtrip.rs` — the hermetic MinIO round-trip `loom_fixture_test`.
- `src/services/runtime/tests/object_store_config.rs` — `ObjectStoreConfig` parsing/validation unit tests (plain `rust_test`).

**Modify:**
- `src/control-plane/postgres/src/iceberg_sql_catalog/mod.rs` — add `pub mod s3_storage;` + re-export `S3StorageFactory`.
- `src/control-plane/postgres/src/fixture.rs` — add `MinioFixture` (spawn/readiness/Drop) + SigV4 `create_bucket`.
- `src/control-plane/postgres/Cargo.toml` + `BUCK` — add `object_store` (aws), `typetag`; test-only `reqwest`/`hmac`/`sha2`/`hex` for the fixture.
- `src/control-plane/postgres/defs.bzl` — add a `minio = False` flag to `loom_fixture_test` (mirrors `duckdb`).
- `src/services/runtime/src/lib.rs` — `ObjectStoreConfig` + `from_map` parsing + `build_storage_factory` + `build_serving_object_store`.
- `src/services/runtime/Cargo.toml` + `BUCK` — add `iceberg`, `object_store` (aws).
- `src/services/ingest/src/main.rs`, `src/services/engine/src/main.rs`, `src/services/query-api/src/main.rs` — call `build_storage_factory` + use `cfg.object_store.warehouse_uri`.
- `src/services/query-api/src/serving_datafusion.rs` — S3-aware object-store registration + scheme-correct scan URL.
- `docs/ROADMAP.md` / `docs/FUTURE.md` — close `road-iceberg-real-object-store`; record deferrals.

---

### Task 1: `S3Storage` / `S3StorageFactory` over `object_store`

Implements iceberg's `Storage`/`StorageFactory` for `s3://` URLs using `object_store::aws::AmazonS3`. Self-contained in the postgres crate; the factory carries all S3 config (so `catalog.rs` is unchanged — it just receives whatever factory is injected).

**Files:**
- Create: `src/control-plane/postgres/src/iceberg_sql_catalog/s3_storage.rs`
- Modify: `src/control-plane/postgres/src/iceberg_sql_catalog/mod.rs`
- Modify: `src/control-plane/postgres/Cargo.toml:6-55` (add deps)
- Modify: `src/control-plane/postgres/BUCK` (`:postgres` lib deps)
- Test: `src/control-plane/postgres/tests/s3_storage.rs`

**Interfaces:**
- Produces:
  - `pub struct S3StorageFactory` with `pub fn new(bucket: String, endpoint: Option<String>, region: String, access_key_id: String, secret_access_key: String, path_style: bool) -> Self`, impl `iceberg::io::StorageFactory`.
  - `pub struct S3Storage` (impl `iceberg::io::Storage`); `pub(crate) fn key_of(path: &str) -> iceberg::Result<object_store::path::Path>` — strips `s3://{bucket}/` (and a bare `s3://`/leading `/`) to an object-store key.
- Consumes (later tasks): `service_runtime::build_storage_factory` constructs `S3StorageFactory`; the round-trip test constructs it directly.

- [ ] **Step 1: Add deps to `Cargo.toml`** (under `[dependencies]`, after `thiserror = "1"` at line 55):

```toml
# S3/MinIO Iceberg storage (src/iceberg_sql_catalog/s3_storage.rs). The `aws`
# feature pulls object_store's AmazonS3 client. reindeer unions features across the
# workspace, so this also enables `aws` on the shared //third-party:object_store the
# services already depend on (the serving S3 read path needs it too).
object_store = { version = "0.13", features = ["aws"] }
# iceberg's Storage/StorageFactory traits are #[typetag::serde]; impls need the attr.
typetag = "0.2"
# Async S3 ObjectStore methods return futures::stream for list(); pull StreamExt.
futures = "0.3"
```

Add to `[dev-dependencies]` (after line 59) — used only by `fixture.rs`'s MinIO bucket creation (Task 7) and tests:

```toml
# AWS SigV4 PUT-bucket in the MinIO fixture (no vendored `mc`). Test/fixture only.
reqwest = { version = "0.12", default-features = false, features = ["rustls-tls"] }
hmac = "0.12"
sha2 = "0.10"
hex = "0.4"
```

- [ ] **Step 2: Regenerate third-party rules**

Run: `eval "$(./tools/env.sh)" && ./tools/buckify.sh`
Expected: `third-party/BUCK` gains `aws`/`cloud` features on `object_store-0.13` (and its new transitive crates: `quick-xml`, `md-5`, `ring`, etc.), a `//third-party:typetag` alias, and `//third-party:futures` if not already present. Commit the diff.

- [ ] **Step 3: Write the failing unit test** — `src/control-plane/postgres/tests/s3_storage.rs`:

```rust
//! Pure-logic tests for the S3 storage adapter — key derivation and typetag serde.
//! Real S3 I/O is proven by the hermetic MinIO round-trip (tests/iceberg_s3_roundtrip.rs).
use control_plane_postgres::iceberg_sql_catalog::s3_storage::{S3Storage, S3StorageFactory};
use iceberg::io::StorageFactory;
use std::sync::Arc;

#[test]
fn key_strips_scheme_and_bucket() {
    // s3://warehouse/loom/orders/data/abc.parquet  ->  loom/orders/data/abc.parquet
    let k = S3Storage::key_of("s3://warehouse/loom/orders/data/abc.parquet").unwrap();
    assert_eq!(k.as_ref(), "loom/orders/data/abc.parquet");
}

#[test]
fn key_handles_bucket_root() {
    let k = S3Storage::key_of("s3://warehouse/metadata/v1.json").unwrap();
    assert_eq!(k.as_ref(), "metadata/v1.json");
}

#[test]
fn factory_builds_storage_via_typetag_trait() {
    let f = S3StorageFactory::new(
        "warehouse".into(),
        Some("http://127.0.0.1:9000".into()),
        "us-east-1".into(),
        "minioadmin".into(),
        "minioadmin".into(),
        true,
    );
    // StorageConfig::new() is empty — the factory carries config, not FileIO props.
    let storage = f.build(&iceberg::io::StorageConfig::new()).unwrap();
    assert!(format!("{storage:?}").contains("S3Storage"));
}

#[test]
fn factory_serde_roundtrips_via_typetag() {
    // The trait is #[typetag::serde]; a boxed factory must serialize with a "type" tag.
    let f: Arc<dyn StorageFactory> = Arc::new(S3StorageFactory::new(
        "wh".into(), None, "us-east-1".into(), "ak".into(), "sk".into(), false,
    ));
    let json = serde_json::to_string(&f).unwrap();
    assert!(json.contains("S3StorageFactory"));
    let back: Box<dyn StorageFactory> = serde_json::from_str(&json).unwrap();
    assert!(format!("{back:?}").contains("S3StorageFactory"));
}
```

- [ ] **Step 4: Add the test target to `BUCK`** (plain `rust_test`, pure-logic → runs on RE):

```python
rust_test(
    name = "s3-storage",
    crate = "s3_storage",
    srcs = ["tests/s3_storage.rs"],
    crate_root = "tests/s3_storage.rs",
    edition = "2024",
    deps = [
        ":postgres",
        "//third-party:iceberg",
        "//third-party:serde_json",
    ],
)
```

Add `object_store`, `typetag`, `futures` to the `:postgres` library target's `deps` list (alphabetically among the existing `//third-party:*` entries).

- [ ] **Step 5: Run the test to verify it fails to compile** (module not yet created)

Run: `buck2 test //src/control-plane/postgres:s3-storage > /tmp/t.log 2>&1; grep -E "error|FAIL|Tests finished" /tmp/t.log`
Expected: compile error — `unresolved import ... s3_storage`.

- [ ] **Step 6: Implement `s3_storage.rs`**

```rust
//! S3-compatible Iceberg storage backend.
//!
//! iceberg 0.9 ships only `file://`/`memory://` `Storage` impls (its `config/s3.rs`
//! is config types only). This module supplies an `s3://` backend over
//! `object_store::aws::AmazonS3`, injected via `SqlCatalogBuilder::with_storage_factory`.
//! Paths flow in as full `s3://{bucket}/{key}` URLs (the warehouse location prefix);
//! `AmazonS3` is bound to one bucket, so we strip the `s3://{bucket}/` prefix to a key.
//!
//! The `Storage`/`StorageFactory` traits are `#[typetag::serde]`, so both types are
//! `Serialize + Deserialize`. The built `AmazonS3` client is non-serializable and
//! non-config state, so it lives behind `#[serde(skip)]` + a lazy `OnceLock`,
//! mirroring iceberg's own `FileIO`.

use std::ops::Range;
use std::sync::{Arc, OnceLock};

use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use iceberg::io::{
    FileMetadata, FileRead, FileWrite, InputFile, OutputFile, Storage, StorageConfig,
    StorageFactory,
};
use iceberg::{Error, ErrorKind, Result};
use object_store::aws::AmazonS3Builder;
use object_store::path::Path as ObjPath;
// `head`/`get`/`put`/`delete`/`get_range` live on the ObjectStoreExt extension trait
// in object_store 0.13 (the base ObjectStore trait only has *_opts/list/get_ranges).
// serving_datafusion.rs imports ObjectStoreExt for the same reason.
use object_store::{GetOptions, GetRange, ObjectStore, ObjectStoreExt, PutPayload};
use serde::{Deserialize, Serialize};

/// S3 connection config shared by the factory and storage. All fields serializable.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct S3Settings {
    bucket: String,
    endpoint: Option<String>,
    region: String,
    access_key_id: String,
    secret_access_key: String,
    /// Path-style addressing (MinIO requires it); virtual-hosted otherwise.
    path_style: bool,
}

impl S3Settings {
    fn build_store(&self) -> Result<AmazonS3> {
        use object_store::aws::AmazonS3;
        let mut b = AmazonS3Builder::new()
            .with_bucket_name(&self.bucket)
            .with_region(&self.region)
            .with_access_key_id(&self.access_key_id)
            .with_secret_access_key(&self.secret_access_key)
            // path_style == true  => NOT virtual-hosted.
            .with_virtual_hosted_style_request(!self.path_style);
        if let Some(ep) = &self.endpoint {
            // MinIO/local endpoints are plain HTTP; allow it.
            b = b.with_endpoint(ep).with_allow_http(true);
        }
        b.build()
            .map_err(|e| Error::new(ErrorKind::Unexpected, format!("build AmazonS3: {e}")))
    }
}

/// Factory that builds [`S3Storage`] for the `s3://` scheme.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct S3StorageFactory {
    settings: S3Settings,
}

impl S3StorageFactory {
    /// Construct from discrete fields (called by `service_runtime::build_storage_factory`).
    pub fn new(
        bucket: String,
        endpoint: Option<String>,
        region: String,
        access_key_id: String,
        secret_access_key: String,
        path_style: bool,
    ) -> Self {
        Self {
            settings: S3Settings {
                bucket,
                endpoint,
                region,
                access_key_id,
                secret_access_key,
                path_style,
            },
        }
    }
}

#[typetag::serde]
impl StorageFactory for S3StorageFactory {
    fn build(&self, _config: &StorageConfig) -> Result<Arc<dyn Storage>> {
        Ok(Arc::new(S3Storage::new(self.settings.clone())))
    }
}

/// S3 storage. Holds config (serializable) + a lazily-built `AmazonS3` (skipped).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct S3Storage {
    settings: S3Settings,
    #[serde(skip)]
    store: Arc<OnceLock<Arc<dyn ObjectStore>>>,
}

impl S3Storage {
    fn new(settings: S3Settings) -> Self {
        Self {
            settings,
            store: Arc::new(OnceLock::new()),
        }
    }

    fn store(&self) -> Result<Arc<dyn ObjectStore>> {
        if let Some(s) = self.store.get() {
            return Ok(s.clone());
        }
        let s: Arc<dyn ObjectStore> = Arc::new(self.settings.build_store()?);
        let _ = self.store.set(s.clone());
        Ok(self.store.get().unwrap().clone())
    }

    /// `s3://{bucket}/{key}` -> `{key}` (also tolerates `s3://{key}` and leading `/`).
    pub(crate) fn key_of(path: &str) -> Result<ObjPath> {
        let rest = path.strip_prefix("s3://").unwrap_or(path);
        // Drop the first path segment (bucket) if present.
        let key = match rest.split_once('/') {
            Some((_bucket, key)) => key,
            None => "",
        };
        Ok(ObjPath::from(key.trim_start_matches('/')))
    }

    fn obj_err(e: object_store::Error) -> Error {
        Error::new(ErrorKind::Unexpected, format!("object_store: {e}"))
    }
}

#[async_trait]
#[typetag::serde]
impl Storage for S3Storage {
    async fn exists(&self, path: &str) -> Result<bool> {
        let key = Self::key_of(path)?;
        match self.store()?.head(&key).await {
            Ok(_) => Ok(true),
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(e) => Err(Self::obj_err(e)),
        }
    }

    async fn metadata(&self, path: &str) -> Result<FileMetadata> {
        let key = Self::key_of(path)?;
        let meta = self.store()?.head(&key).await.map_err(Self::obj_err)?;
        Ok(FileMetadata {
            size: meta.size,
        })
    }

    async fn read(&self, path: &str) -> Result<Bytes> {
        let key = Self::key_of(path)?;
        let r = self.store()?.get(&key).await.map_err(Self::obj_err)?;
        r.bytes().await.map_err(Self::obj_err)
    }

    async fn reader(&self, path: &str) -> Result<Box<dyn FileRead>> {
        Ok(Box::new(S3FileRead {
            store: self.store()?,
            key: Self::key_of(path)?,
        }))
    }

    async fn write(&self, path: &str, bs: Bytes) -> Result<()> {
        let key = Self::key_of(path)?;
        self.store()?
            .put(&key, PutPayload::from_bytes(bs))
            .await
            .map(|_| ())
            .map_err(Self::obj_err)
    }

    async fn writer(&self, path: &str) -> Result<Box<dyn FileWrite>> {
        Ok(Box::new(S3FileWrite {
            store: self.store()?,
            key: Self::key_of(path)?,
            buf: Vec::new(),
            closed: false,
        }))
    }

    async fn delete(&self, path: &str) -> Result<()> {
        let key = Self::key_of(path)?;
        match self.store()?.delete(&key).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(e) => Err(Self::obj_err(e)),
        }
    }

    async fn delete_prefix(&self, path: &str) -> Result<()> {
        let prefix = Self::key_of(path)?;
        let store = self.store()?;
        let mut stream = store.list(Some(&prefix));
        while let Some(item) = stream.next().await {
            let meta = item.map_err(Self::obj_err)?;
            store.delete(&meta.location).await.map_err(Self::obj_err)?;
        }
        Ok(())
    }

    fn new_input(&self, path: &str) -> Result<InputFile> {
        Ok(InputFile::new(Arc::new(self.clone()), path.to_string()))
    }

    fn new_output(&self, path: &str) -> Result<OutputFile> {
        Ok(OutputFile::new(Arc::new(self.clone()), path.to_string()))
    }
}

/// Range reader: each `read(range)` is an S3 ranged GET.
#[derive(Debug)]
struct S3FileRead {
    store: Arc<dyn ObjectStore>,
    key: ObjPath,
}

#[async_trait]
impl FileRead for S3FileRead {
    async fn read(&self, range: Range<u64>) -> Result<Bytes> {
        let opts = GetOptions {
            range: Some(GetRange::Bounded(range)),
            ..Default::default()
        };
        let r = self
            .store
            .get_opts(&self.key, opts)
            .await
            .map_err(S3Storage::obj_err)?;
        r.bytes().await.map_err(S3Storage::obj_err)
    }
}

/// Buffered writer: accumulate in memory, single PUT on close. Iceberg metadata and
/// (test-sized) data files are modest; multipart upload for large files is a deferred
/// optimization ([[fut-iceberg-real-object-store]] follow-up).
#[derive(Debug)]
struct S3FileWrite {
    store: Arc<dyn ObjectStore>,
    key: ObjPath,
    buf: Vec<u8>,
    closed: bool,
}

#[async_trait]
impl FileWrite for S3FileWrite {
    async fn write(&mut self, bs: Bytes) -> Result<()> {
        if self.closed {
            return Err(Error::new(ErrorKind::DataInvalid, "write after close"));
        }
        self.buf.extend_from_slice(&bs);
        Ok(())
    }

    async fn close(&mut self) -> Result<()> {
        if self.closed {
            return Err(Error::new(ErrorKind::DataInvalid, "double close"));
        }
        self.closed = true;
        let payload = PutPayload::from_bytes(Bytes::from(std::mem::take(&mut self.buf)));
        self.store
            .put(&self.key, payload)
            .await
            .map(|_| ())
            .map_err(S3Storage::obj_err)
    }
}
```

> Note for implementer: verify `ObjectMeta::size` is `u64` in object_store 0.13.2 (it is — `FileMetadata.size` is also `u64`). If `get_range`/`GetRange` paths differ, the canonical fallback is `store.get_opts(&key, GetOptions { range: Some(GetRange::Bounded(start..end)), ..Default::default() })`. Confirm `AmazonS3` (the concrete type) is imported for `S3Settings::build_store`'s return; if the local `use object_store::aws::AmazonS3;` inside the fn is awkward, hoist it to the module `use`.

- [ ] **Step 7: Wire the module** — `src/control-plane/postgres/src/iceberg_sql_catalog/mod.rs`, add alongside the existing exports:

```rust
pub mod s3_storage;
pub use s3_storage::S3StorageFactory;
```

- [ ] **Step 8: Run tests to verify they pass**

Run: `buck2 test //src/control-plane/postgres:s3-storage > /tmp/t.log 2>&1; grep -E "FAIL|Tests finished" /tmp/t.log`
Expected: `Tests finished: Pass 4. Fail 0.`

- [ ] **Step 9: Clippy + commit**

Run: `buck2 build '//src/control-plane/postgres:postgres[clippy.txt]' > /tmp/c.log 2>&1; cat /tmp/c.log` (expect empty)

```bash
git add src/control-plane/postgres/src/iceberg_sql_catalog/s3_storage.rs \
  src/control-plane/postgres/src/iceberg_sql_catalog/mod.rs \
  src/control-plane/postgres/tests/s3_storage.rs \
  src/control-plane/postgres/{Cargo.toml,BUCK} third-party/BUCK Cargo.lock
git commit -m "feat(iceberg): S3Storage/S3StorageFactory over object_store"
```

---

### Task 2: `ObjectStoreConfig` env parsing + validation in `service_runtime`

`LOOM_WAREHOUSE_URI` scheme-selects the backend; `AWS_*` carry creds. Unset ⇒ `file://{LOOM_DATA_PATH}` (back-compat). Missing creds on an `s3://` warehouse is a fail-fast boot error.

**Files:**
- Modify: `src/services/runtime/src/lib.rs` (add types + parse in `from_map`)
- Test: `src/services/runtime/tests/object_store_config.rs`
- Modify: `src/services/runtime/BUCK` (new test target)

**Interfaces:**
- Produces:
  - `pub struct ObjectStoreConfig { pub warehouse_uri: String, pub backend: ObjectStoreBackend }`
  - `pub enum ObjectStoreBackend { Local, S3(S3Backend) }`
  - `pub struct S3Backend { pub bucket: String, pub endpoint: Option<String>, pub region: String, pub access_key_id: String, pub secret_access_key: String, pub path_style: bool }`
  - `Config` gains `pub object_store: ObjectStoreConfig`.
- Consumes: existing `ConfigError::{MissingVar, Invalid}`, `data_path`.

- [ ] **Step 1: Write the failing test** — `src/services/runtime/tests/object_store_config.rs`:

```rust
use std::collections::HashMap;
use service_runtime::{Config, ObjectStoreBackend};

fn base() -> HashMap<String, String> {
    let mut m = HashMap::new();
    m.insert("LOOM_BIND_ADDR".into(), "127.0.0.1:8080".into());
    m.insert("LOOM_DB_HOST".into(), "/sock".into());
    m.insert("LOOM_DB_PORT".into(), "5432".into());
    m.insert("LOOM_DB_USER".into(), "u".into());
    m.insert("LOOM_DB_PASSWORD".into(), "p".into());
    m.insert("LOOM_DB_NAME".into(), "d".into());
    m.insert("LOOM_DATA_PATH".into(), "/data".into());
    m
}

#[test]
fn unset_warehouse_defaults_to_file_uri_and_local_backend() {
    let cfg = Config::from_map(&base()).unwrap();
    assert_eq!(cfg.object_store.warehouse_uri, "file:///data");
    assert!(matches!(cfg.object_store.backend, ObjectStoreBackend::Local));
}

#[test]
fn explicit_file_uri_is_local() {
    let mut m = base();
    m.insert("LOOM_WAREHOUSE_URI".into(), "file:///warehouse".into());
    let cfg = Config::from_map(&m).unwrap();
    assert!(matches!(cfg.object_store.backend, ObjectStoreBackend::Local));
}

#[test]
fn s3_uri_with_creds_and_endpoint_parses_path_style() {
    let mut m = base();
    m.insert("LOOM_WAREHOUSE_URI".into(), "s3://warehouse/loom".into());
    m.insert("AWS_ENDPOINT_URL".into(), "http://127.0.0.1:9000".into());
    m.insert("AWS_ACCESS_KEY_ID".into(), "ak".into());
    m.insert("AWS_SECRET_ACCESS_KEY".into(), "sk".into());
    let cfg = Config::from_map(&m).unwrap();
    match cfg.object_store.backend {
        ObjectStoreBackend::S3(s) => {
            assert_eq!(s.bucket, "warehouse");
            assert_eq!(s.endpoint.as_deref(), Some("http://127.0.0.1:9000"));
            assert_eq!(s.region, "us-east-1"); // default when endpoint set
            assert_eq!(s.access_key_id, "ak");
            assert!(s.path_style); // endpoint set => path-style
        }
        _ => panic!("expected S3 backend"),
    }
}

#[test]
fn s3_uri_missing_credentials_is_boot_error() {
    let mut m = base();
    m.insert("LOOM_WAREHOUSE_URI".into(), "s3://warehouse/loom".into());
    // no AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY
    assert!(Config::from_map(&m).is_err());
}

#[test]
fn unknown_scheme_is_invalid() {
    let mut m = base();
    m.insert("LOOM_WAREHOUSE_URI".into(), "gs://bucket/x".into());
    assert!(Config::from_map(&m).is_err());
}
```

- [ ] **Step 2: Add the test target** — `src/services/runtime/BUCK`:

```python
rust_test(
    name = "object-store-config",
    crate = "object_store_config",
    srcs = ["tests/object_store_config.rs"],
    crate_root = "tests/object_store_config.rs",
    edition = "2024",
    deps = [":runtime"],
)
```

- [ ] **Step 3: Run to verify failure**

Run: `buck2 test //src/services/runtime:object-store-config > /tmp/t.log 2>&1; grep -E "error|FAIL|Tests finished" /tmp/t.log`
Expected: compile error — `ObjectStoreBackend` / `object_store` field unknown.

- [ ] **Step 4: Implement** — in `src/services/runtime/src/lib.rs`, add the types after `Config` (after line 82) and a parse helper, then a field + parse call in `from_map`.

Types:

```rust
/// Object-store backend for the Iceberg warehouse, selected by `LOOM_WAREHOUSE_URI`'s
/// scheme. `file://` (or unset) => local disk; `s3://bucket/prefix` => S3/MinIO.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObjectStoreConfig {
    /// Base warehouse URI passed to the Iceberg catalog (the `warehouse` prop).
    pub warehouse_uri: String,
    pub backend: ObjectStoreBackend,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ObjectStoreBackend {
    Local,
    S3(S3Backend),
}

/// Resolved S3/MinIO connection settings (from `AWS_*` env + the `s3://` URI's bucket).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct S3Backend {
    pub bucket: String,
    pub endpoint: Option<String>,
    pub region: String,
    pub access_key_id: String,
    pub secret_access_key: String,
    pub path_style: bool,
}

impl ObjectStoreConfig {
    /// Parse from the env map. `data_path` is the back-compat default warehouse root.
    fn parse(
        vars: &HashMap<String, String>,
        data_path: &Path,
    ) -> Result<ObjectStoreConfig, ConfigError> {
        let warehouse_uri = vars
            .get("LOOM_WAREHOUSE_URI")
            .cloned()
            .unwrap_or_else(|| format!("file://{}", data_path.display()));

        let backend = if warehouse_uri.starts_with("file://") {
            ObjectStoreBackend::Local
        } else if let Some(rest) = warehouse_uri.strip_prefix("s3://") {
            let bucket = rest
                .split('/')
                .next()
                .filter(|b| !b.is_empty())
                .ok_or_else(|| ConfigError::Invalid {
                    var: "LOOM_WAREHOUSE_URI".into(),
                    detail: "s3:// URI must include a bucket (s3://bucket/prefix)".into(),
                })?
                .to_string();
            let req = |k: &str| {
                vars.get(k)
                    .cloned()
                    .filter(|v| !v.is_empty())
                    .ok_or_else(|| ConfigError::MissingVar(k.to_string()))
            };
            let endpoint = vars
                .get("AWS_ENDPOINT_URL")
                .cloned()
                .filter(|v| !v.is_empty());
            let region = vars
                .get("AWS_REGION")
                .cloned()
                .filter(|v| !v.is_empty())
                .unwrap_or_else(|| "us-east-1".to_string());
            ObjectStoreBackend::S3(S3Backend {
                bucket,
                // Path-style is required by MinIO; implied whenever an endpoint is set.
                path_style: endpoint.is_some(),
                endpoint,
                region,
                access_key_id: req("AWS_ACCESS_KEY_ID")?,
                secret_access_key: req("AWS_SECRET_ACCESS_KEY")?,
            })
        } else {
            return Err(ConfigError::Invalid {
                var: "LOOM_WAREHOUSE_URI".into(),
                detail: format!("unsupported scheme in {warehouse_uri}; use file:// or s3://"),
            });
        };
        Ok(ObjectStoreConfig {
            warehouse_uri,
            backend,
        })
    }
}
```

In `Config` struct (after `pub data_path: PathBuf,` at line 80) add:

```rust
    pub object_store: ObjectStoreConfig,
```

In `from_map`, compute `data_path` first, then parse object_store before building `Config`. Change the `data_path` line and the struct literal (around lines 119-130):

```rust
        let data_path = PathBuf::from(req("LOOM_DATA_PATH")?);
        let object_store = ObjectStoreConfig::parse(vars, &data_path)?;

        Ok(Config {
            bind_addr,
            db: DbConfig {
                host: req("LOOM_DB_HOST")?,
                port,
                user: req("LOOM_DB_USER")?,
                password: req("LOOM_DB_PASSWORD")?,
                dbname: req("LOOM_DB_NAME")?,
            },
            data_path,
            object_store,
            lock_timeout,
        })
```

- [ ] **Step 5: Run to verify pass**

Run: `buck2 test //src/services/runtime:object-store-config > /tmp/t.log 2>&1; grep -E "FAIL|Tests finished" /tmp/t.log`
Expected: `Tests finished: Pass 5. Fail 0.`

- [ ] **Step 6: Commit**

```bash
git add src/services/runtime/src/lib.rs src/services/runtime/{BUCK} \
  src/services/runtime/tests/object_store_config.rs
git commit -m "feat(runtime): ObjectStoreConfig env parsing + validation"
```

---

### Task 3: `build_storage_factory` + `build_serving_object_store` seams

`build_storage_factory` returns the iceberg `StorageFactory` the catalog needs; `build_serving_object_store` returns the DataFusion serving read store (bucket + `Arc<dyn ObjectStore>`), `None` for local.

**Files:**
- Modify: `src/services/runtime/src/lib.rs`
- Modify: `src/services/runtime/Cargo.toml` (+ `iceberg`, `object_store` aws), then `./tools/buckify.sh`
- Modify: `src/services/runtime/BUCK` (`:runtime` deps += `//third-party:iceberg`, postgres already a dep)
- Test: extend `src/services/runtime/tests/object_store_config.rs`

**Interfaces:**
- Produces:
  - `pub fn build_storage_factory(cfg: &ObjectStoreConfig) -> Result<Arc<dyn iceberg::io::StorageFactory>, ConfigError>`
  - `pub fn build_serving_object_store(cfg: &ObjectStoreConfig) -> Result<Option<(String, Arc<dyn object_store::ObjectStore>)>, RuntimeError>` — `Some((bucket, store))` for S3, `None` for local.
- Consumes: `S3StorageFactory` (Task 1), `iceberg::io::LocalFsStorageFactory`, `object_store::aws::AmazonS3Builder`.

- [ ] **Step 1: Add deps** — `src/services/runtime/Cargo.toml`:

```toml
# build_storage_factory injects the iceberg StorageFactory (Local or S3); the S3 impl
# lives in control-plane-postgres. object_store(aws) builds the AmazonS3 serving store.
iceberg = "0.9"
object_store = { version = "0.13", features = ["aws"] }
```

Run: `eval "$(./tools/env.sh)" && ./tools/buckify.sh`. Add `//third-party:iceberg` to `:runtime` deps in `BUCK` (object_store already present).

- [ ] **Step 2: Write the failing test** — append to `tests/object_store_config.rs`:

```rust
use service_runtime::{build_serving_object_store, build_storage_factory};

#[test]
fn build_storage_factory_local_for_file_backend() {
    let cfg = Config::from_map(&base()).unwrap();
    let f = build_storage_factory(&cfg.object_store).unwrap();
    assert!(format!("{f:?}").contains("LocalFsStorageFactory"));
    assert!(build_serving_object_store(&cfg.object_store).unwrap().is_none());
}

#[test]
fn build_storage_factory_s3_for_s3_backend() {
    let mut m = base();
    m.insert("LOOM_WAREHOUSE_URI".into(), "s3://warehouse/loom".into());
    m.insert("AWS_ENDPOINT_URL".into(), "http://127.0.0.1:9000".into());
    m.insert("AWS_ACCESS_KEY_ID".into(), "ak".into());
    m.insert("AWS_SECRET_ACCESS_KEY".into(), "sk".into());
    let cfg = Config::from_map(&m).unwrap();
    let f = build_storage_factory(&cfg.object_store).unwrap();
    assert!(format!("{f:?}").contains("S3StorageFactory"));
    let (bucket, _store) = build_serving_object_store(&cfg.object_store).unwrap().unwrap();
    assert_eq!(bucket, "warehouse");
}
```

- [ ] **Step 3: Run to verify failure**

Run: `buck2 test //src/services/runtime:object-store-config > /tmp/t.log 2>&1; grep -E "error|FAIL" /tmp/t.log`
Expected: unresolved `build_storage_factory` / `build_serving_object_store`.

- [ ] **Step 4: Implement** — in `src/services/runtime/src/lib.rs` (add `use std::sync::Arc;` to imports if absent):

```rust
/// Build the Iceberg `StorageFactory` the SQL catalog uses for metadata/data I/O.
pub fn build_storage_factory(
    cfg: &ObjectStoreConfig,
) -> Result<Arc<dyn iceberg::io::StorageFactory>, ConfigError> {
    use control_plane_postgres::iceberg_sql_catalog::S3StorageFactory;
    use iceberg::io::LocalFsStorageFactory;
    match &cfg.backend {
        ObjectStoreBackend::Local => Ok(Arc::new(LocalFsStorageFactory)),
        ObjectStoreBackend::S3(s) => Ok(Arc::new(S3StorageFactory::new(
            s.bucket.clone(),
            s.endpoint.clone(),
            s.region.clone(),
            s.access_key_id.clone(),
            s.secret_access_key.clone(),
            s.path_style,
        ))),
    }
}

/// Build the DataFusion serving read store for `s3://` warehouses. Returns the bucket
/// name (for the `ObjectStoreUrl`) + the store, or `None` for local-filesystem reads.
pub fn build_serving_object_store(
    cfg: &ObjectStoreConfig,
) -> Result<Option<(String, Arc<dyn object_store::ObjectStore>)>, RuntimeError> {
    match &cfg.backend {
        ObjectStoreBackend::Local => Ok(None),
        ObjectStoreBackend::S3(s) => {
            let mut b = object_store::aws::AmazonS3Builder::new()
                .with_bucket_name(&s.bucket)
                .with_region(&s.region)
                .with_access_key_id(&s.access_key_id)
                .with_secret_access_key(&s.secret_access_key)
                .with_virtual_hosted_style_request(!s.path_style);
            if let Some(ep) = &s.endpoint {
                b = b.with_endpoint(ep).with_allow_http(true);
            }
            let store = b.build().map_err(RuntimeError::Store)?;
            Ok(Some((s.bucket.clone(), Arc::new(store))))
        }
    }
}
```

- [ ] **Step 5: Run to verify pass**

Run: `buck2 test //src/services/runtime:object-store-config > /tmp/t.log 2>&1; grep -E "FAIL|Tests finished" /tmp/t.log`
Expected: `Tests finished: Pass 7. Fail 0.`

- [ ] **Step 6: Clippy + commit**

Run: `buck2 build '//src/services/runtime:runtime[clippy.txt]' > /tmp/c.log 2>&1; cat /tmp/c.log` (expect empty)

```bash
git add src/services/runtime/{src/lib.rs,Cargo.toml,BUCK} \
  src/services/runtime/tests/object_store_config.rs third-party/BUCK Cargo.lock
git commit -m "feat(runtime): build_storage_factory + build_serving_object_store seams"
```

---

### Task 4: Wire the three service `main.rs` files

Replace the hardcoded `LocalFsStorageFactory` + `file://` warehouse with the config-driven seam. For unset env this is behavior-identical.

**Files:**
- Modify: `src/services/ingest/src/main.rs:69-83`
- Modify: `src/services/engine/src/main.rs:25-42`
- Modify: `src/services/query-api/src/main.rs:87-101`

**Interfaces:**
- Consumes: `service_runtime::build_storage_factory`, `cfg.object_store.warehouse_uri`, `SQL_CATALOG_PROP_WAREHOUSE`.

There is no per-`main` unit test surface (these are thin wiring fns proven by the existing fixture tests + the Task 9 round-trip). This task ends with a full build.

- [ ] **Step 1: ingest** — `src/services/ingest/src/main.rs`. Remove `use iceberg::io::LocalFsStorageFactory;` (line 11). In `build_iceberg_catalog` (lines 74-83) replace the warehouse insert + builder:

```rust
    props.insert(
        SQL_CATALOG_PROP_WAREHOUSE.to_string(),
        cfg.object_store.warehouse_uri.clone(),
    );
    let catalog = SqlCatalogBuilder::default()
        .with_storage_factory(service_runtime::build_storage_factory(&cfg.object_store)?)
        .load("loom", props)
        .await?;
    Ok(catalog)
```

(`Arc` may now be unused in this file — remove its `use` if clippy flags it.)

- [ ] **Step 2: engine** — `src/services/engine/src/main.rs`. Remove `use iceberg::io::LocalFsStorageFactory;` (line 13). Replace the warehouse insert (lines 27-30) and BOTH builder calls (lines 34-41):

```rust
    props.insert(
        SQL_CATALOG_PROP_WAREHOUSE.to_string(),
        cfg.object_store.warehouse_uri.clone(),
    );

    // Build two SqlCatalog instances from the same props — SqlCatalog is not Clone.
    let catalog = SqlCatalogBuilder::default()
        .with_storage_factory(service_runtime::build_storage_factory(&cfg.object_store)?)
        .load("loom", props.clone())
        .await?;
    let flight_catalog = SqlCatalogBuilder::default()
        .with_storage_factory(service_runtime::build_storage_factory(&cfg.object_store)?)
        .load("loom", props)
        .await?;
```

- [ ] **Step 3: query-api** — `src/services/query-api/src/main.rs`. Remove `use iceberg::io::LocalFsStorageFactory;` (line 15). In `build_iceberg_catalog` (lines 91-100) apply the same change as ingest (warehouse from `cfg.object_store.warehouse_uri`, factory from `build_storage_factory`).

- [ ] **Step 4: Build all three binaries**

Run: `buck2 build //src/services/ingest:ingest-bin //src/services/engine:engine-bin //src/services/query-api:query-api-bin > /tmp/b.log 2>&1; grep -E "BUILD (SUCCEEDED|FAILED)|error" /tmp/b.log`
Expected: `BUILD SUCCEEDED`.

- [ ] **Step 5: Regression — existing iceberg fixture tests still green** (file:// path unchanged)

Run: `buck2 test //src/control-plane/postgres:iceberg-writer //src/control-plane/postgres:iceberg-landing //src/control-plane/postgres:iceberg-flush > /tmp/t.log 2>&1; grep -E "FAIL|Tests finished" /tmp/t.log`
Expected: all pass.

- [ ] **Step 6: Commit**

```bash
git add src/services/{ingest,engine,query-api}/src/main.rs
git commit -m "feat(services): drive Iceberg warehouse/factory from ObjectStoreConfig"
```

---

### Task 5: Make the DataFusion serving read path S3-aware

`serving_datafusion.rs` hardwires `ObjectStoreUrl::local_filesystem()` (register at `:99-102`, scan at `:457`). Thread the optional S3 store through `DataFusionServingEngine`, register it when present, and derive the scan's `ObjectStoreUrl` from each file's scheme.

**Files:**
- Modify: `src/services/query-api/src/serving_datafusion.rs`
- Modify: `src/services/query-api/src/main.rs:70` (pass the serving store)

**Interfaces:**
- Changes: `DataFusionServingEngine::new(catalog: IcebergCatalog, serving_store: Option<(String, Arc<dyn object_store::ObjectStore>)>) -> Self`.
- Consumes: `service_runtime::build_serving_object_store`.

- [ ] **Step 1: Write the failing test** — extend the round-trip coverage at Task 9; this task has no isolated unit test (it requires a live object store). Mark the change spec-driven and rely on Task 9's assertion that `s3://` rows read back through `DataFusionServingEngine`. Proceed to implement, with the build + Task 9 as the gate.

- [ ] **Step 2: Add a struct field** — `serving_datafusion.rs` around line 51-67. Add the field to `DataFusionServingEngine` and a helper to compute the scan URL:

```rust
pub struct DataFusionServingEngine {
    catalog: IcebergCatalog,
    /// `Some((bucket, store))` for an S3 warehouse; `None` => local filesystem.
    serving_store: Option<(String, Arc<dyn object_store::ObjectStore>)>,
}

impl DataFusionServingEngine {
    pub fn new(
        catalog: IcebergCatalog,
        serving_store: Option<(String, Arc<dyn object_store::ObjectStore>)>,
    ) -> Self {
        Self {
            catalog,
            serving_store,
        }
    }
    // ... existing methods; the query method passes self.serving_store.as_ref() to
    // register_iceberg_table (see Step 3).
}

/// The object-store URL a data file's absolute path resolves against. `s3://bucket/...`
/// => `s3://bucket`; everything else (absolute `file://`/local paths) => local filesystem.
fn object_store_url_for(path: &str) -> datafusion::error::Result<ObjectStoreUrl> {
    if let Some(rest) = path.strip_prefix("s3://") {
        let bucket = rest.split('/').next().unwrap_or("");
        ObjectStoreUrl::parse(format!("s3://{bucket}"))
    } else {
        Ok(ObjectStoreUrl::local_filesystem())
    }
}
```

- [ ] **Step 3: Thread the store into `register_iceberg_table`.** Change its signature to accept the optional serving store and register it. At the call site (line 69) pass `self.serving_store.as_ref()`. In the body (lines 90-102) keep the local registration and add S3:

```rust
pub async fn register_iceberg_table(
    ctx: &SessionContext,
    catalog: &IcebergCatalog,
    table: &TableRef,
    serving_store: Option<&(String, Arc<dyn object_store::ObjectStore>)>,
) -> Result<(), ServingError> {
    use control_plane_core::Catalog;

    // Local store for absolute file:// warehouse paths (back-compat default).
    ctx.register_object_store(
        ObjectStoreUrl::local_filesystem().as_ref(),
        Arc::new(LocalFileSystem::new()),
    );
    // S3 store for s3:// warehouse paths, registered under s3://{bucket}.
    if let Some((bucket, store)) = serving_store {
        let url = ObjectStoreUrl::parse(format!("s3://{bucket}")).map_err(to_serving)?;
        ctx.register_object_store(url.as_ref(), store.clone());
    }
```

- [ ] **Step 4: Pass the scan URL into the provider.** `IcebergMirrorTableProvider` must use the scheme-correct URL at `scan` (line 457). The provider already holds `self.files`; derive the URL from the first kept file (fall back to local when empty):

```rust
        let kept = prune_files(&self.schema, filters, &self.files);
        let source = Arc::new(ParquetSource::new(self.schema.clone()));
        let store_url = match kept.first() {
            Some(f) => object_store_url_for(&f.path)?,
            None => ObjectStoreUrl::local_filesystem(),
        };
        let mut builder = FileScanConfigBuilder::new(store_url, source).with_limit(limit);
```

The per-file `ListingTableUrl::parse(&f.path)` + `url.prefix()` already yields the correct object-store key for both `file://` and `s3://` URLs — no change to the loop body.

**Update ALL call sites of `register_iceberg_table` (the signature gained a 4th arg).** Besides the production caller (`serving_datafusion.rs:69` → pass `self.serving_store.as_ref()`), there are **three test call sites** that must each pass `None` (they exercise the local path):
- `src/services/query-api/tests/datafusion_register.rs:34` → `register_iceberg_table(&ctx, &catalog, &table, None)`
- `src/services/query-api/tests/iceberg_pruning_e2e.rs:66` → `register_iceberg_table(&ctx, &catalog, &table, None)`
- `src/services/query-api/tests/iceberg_schema_evolution_read.rs:140` → `register_iceberg_table(&ctx, &cat, &t, None)`

`serving_store: Option<&(String, Arc<dyn object_store::ObjectStore>)>`, so `None` type-checks directly. `IcebergMirrorTableProvider`'s URL derivation lives entirely inside `scan`, so no constructor change is needed.

- [ ] **Step 5: Update `query-api` main** — `src/services/query-api/src/main.rs:70`:

```rust
                Arc::new(DataFusionServingEngine::new(
                    IcebergCatalog::new(pool),
                    service_runtime::build_serving_object_store(&cfg.object_store)?,
                )),
```

- [ ] **Step 6: Build + existing query-api e2e regression**

Run: `buck2 build //src/services/query-api:query-api-bin > /tmp/b.log 2>&1; grep -E "BUILD (SUCCEEDED|FAILED)|error\[" /tmp/b.log`
Run: `buck2 test //src/services/query-api/... > /tmp/t.log 2>&1; grep -E "FAIL|Tests finished" /tmp/t.log`
Expected: build succeeds; all existing serving tests still pass (local path unchanged). In particular the three call-site-touched fixture targets must be green: `datafusion-register`, `iceberg-pruning-e2e`, `iceberg-schema-evolution-read` (a compile error here means a call site was missed in Step 3).

- [ ] **Step 7: Clippy + commit**

Run: `buck2 build '//src/services/query-api:query-api[clippy.txt]' > /tmp/c.log 2>&1; cat /tmp/c.log` (expect empty)

```bash
git add src/services/query-api/src/{serving_datafusion.rs,main.rs}
git commit -m "feat(query-api): S3-aware DataFusion serving read path"
```

---

### Task 6: Vendor a pinned MinIO server binary in buck

Use the per-arch `command_alias` + `select(prelude//cpu/constraints:…)` pattern from `tools/BUCK` (jq/reindeer), each arch a raw static binary via `http_file` + `genrule` (cp+chmod). MinIO publishes raw `linux-amd64`/`linux-arm64` server binaries. (Note: the existing `duckdb-cli` target in `postgres/BUCK` is a *single x86_64-only* genrule with `labels = ["uses_xz"]`; the round-trip is local-only/linux, so a single-arch genrule would also suffice — but the `command_alias`+`select` form below is portable across the repo's two RE arches and is the cleaner precedent to follow. `command_alias` is a prelude global; no `load` needed.)

**Files:**
- Modify: `src/control-plane/postgres/BUCK` (add `minio-bin` near the `duckdb-cli` target ~line 54-72)

**Interfaces:**
- Produces: `//src/control-plane/postgres:minio-bin` (an executable target resolving to the `minio` binary).

- [ ] **Step 1: Pick a pinned release and compute sha256s.** Choose a current stable MinIO release tag (format `RELEASE.YYYY-MM-DDTHH-MM-SSZ`). Download both arch binaries and hash them:

```bash
REL="RELEASE.2025-04-22T22-12-26Z"   # implementer: confirm latest stable at build time
for A in amd64 arm64; do
  curl -fsSL "https://dl.min.io/server/minio/release/linux-$A/archive/minio.$REL" -o /tmp/minio.$A
  echo "$A: $(sha256sum /tmp/minio.$A | cut -d' ' -f1)"
done
```

- [ ] **Step 2: Add the buck targets** — `src/control-plane/postgres/BUCK` (fill the two `sha256` from Step 1):

```python
MINIO_RELEASE = "RELEASE.2025-04-22T22-12-26Z"
_MINIO_URL = "https://dl.min.io/server/minio/release/linux-{arch}/archive/minio.{rel}"

http_file(
    name = "minio-amd64.bin",
    urls = [_MINIO_URL.format(arch = "amd64", rel = MINIO_RELEASE)],
    sha256 = "<amd64-sha256>",
)
http_file(
    name = "minio-arm64.bin",
    urls = [_MINIO_URL.format(arch = "arm64", rel = MINIO_RELEASE)],
    sha256 = "<arm64-sha256>",
)
genrule(
    name = "minio-x86_64-linux",
    out = "minio",
    cmd = "cp $(location :minio-amd64.bin) $OUT && chmod +x $OUT",
    executable = True,
)
genrule(
    name = "minio-aarch64-linux",
    out = "minio",
    cmd = "cp $(location :minio-arm64.bin) $OUT && chmod +x $OUT",
    executable = True,
)
command_alias(
    name = "minio-bin",
    exe = select({
        "prelude//cpu/constraints:x86_64": ":minio-x86_64-linux",
        "prelude//cpu/constraints:arm64": ":minio-aarch64-linux",
    }),
    visibility = ["PUBLIC"],
)
```

> Note: the round-trip test consumes this via `$(location //src/control-plane/postgres:minio-bin)` in its `env`; `command_alias` resolves to the concrete per-arch genrule (the same pattern jq uses). If `$(location)` on a `command_alias` proves awkward in a test `env`, reference the per-arch genrule through a `select` in the test's `env` instead (mirror how `duckdb-cli` is passed).

- [ ] **Step 3: Verify the binary materializes**

Run: `buck2 build //src/control-plane/postgres:minio-bin > /tmp/b.log 2>&1; grep -E "BUILD (SUCCEEDED|FAILED)" /tmp/b.log`
Expected: `BUILD SUCCEEDED`.

- [ ] **Step 4: Commit**

```bash
git add src/control-plane/postgres/BUCK
git commit -m "build(test): vendor pinned MinIO server binary"
```

---

### Task 7: `MinioFixture` (spawn/readiness/Drop) + SigV4 bucket creation

Mirror `PgFixture`: pick a free port, spawn `minio server` against a tempdir with known root creds, poll readiness, tear down on Drop. Create the warehouse bucket with a minimal AWS SigV4 `PUT /{bucket}` (no `mc`).

**Files:**
- Modify: `src/control-plane/postgres/src/fixture.rs` (add `MinioFixture` + SigV4 helper)

**Interfaces:**
- Produces:
  - `pub struct MinioFixture` with `pub fn start() -> Self`, `pub fn endpoint(&self) -> String` (e.g. `http://127.0.0.1:PORT`), `pub fn access_key(&self) -> &str`, `pub fn secret_key(&self) -> &str`, and `pub async fn create_bucket(&self, bucket: &str)`.
- Consumes: `MINIO_BIN` env (Task 8), `tempfile::TempDir`, `std::process::{Child, Command}`, dev-deps `reqwest`/`hmac`/`sha2`/`hex`, existing `time` dep for the SigV4 timestamp.

- [ ] **Step 1: Implement `MinioFixture`** — append to `fixture.rs`. Use creds `minioadmin`/`minioadmin` (MinIO default root). Pick a free port by binding `TcpListener` to `127.0.0.1:0` and reading the port, then dropping the listener before launch (same race-tolerant trick used widely; MinIO rebinds immediately).

```rust
/// A running ephemeral MinIO server. Killed and cleaned up on drop. Booted from the
/// vendored `minio` binary (path via `MINIO_BIN`); like `initdb` it won't run as root
/// on RE, so consumers must be `loom_fixture_test` targets.
pub struct MinioFixture {
    _data_dir: TempDir,
    server: Child,
    endpoint: String,
}

impl MinioFixture {
    pub fn start() -> Self {
        let bin = std::env::var("MINIO_BIN").expect("MINIO_BIN must point at the minio binary");
        let data_dir = tempfile::tempdir().expect("minio data tempdir");
        // Reserve a free port, then release it for minio to claim.
        let port = {
            let l = std::net::TcpListener::bind("127.0.0.1:0").expect("reserve port");
            l.local_addr().unwrap().port()
        };
        let addr = format!("127.0.0.1:{port}");
        let server = std::process::Command::new(&bin)
            .arg("server")
            .arg(data_dir.path())
            .args(["--address", &addr])
            .env("MINIO_ROOT_USER", "minioadmin")
            .env("MINIO_ROOT_PASSWORD", "minioadmin")
            // Quiet, no update checks, no console.
            .env("MINIO_UPDATE", "off")
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .expect("spawn minio");
        let endpoint = format!("http://{addr}");
        let fixture = Self {
            _data_dir: data_dir,
            server,
            endpoint,
        };
        fixture.wait_ready();
        fixture
    }

    pub fn endpoint(&self) -> String {
        self.endpoint.clone()
    }
    pub fn access_key(&self) -> &str {
        "minioadmin"
    }
    pub fn secret_key(&self) -> &str {
        "minioadmin"
    }

    fn wait_ready(&self) {
        // Poll the unauthenticated liveness probe up to ~15s.
        let url = format!("{}/minio/health/live", self.endpoint);
        let client = reqwest::blocking::Client::new();
        for _ in 0..300 {
            if let Ok(r) = client.get(&url).send() {
                if r.status().is_success() {
                    return;
                }
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        panic!("minio did not become ready within ~15s");
    }
```

> If `reqwest`'s `blocking` client is undesirable in the fixture, poll readiness with a raw `TcpStream::connect` to the port instead, and use the async `reqwest::Client` only in `create_bucket`. Implementer's choice; add the `blocking` feature to the dev-dep only if used.

- [ ] **Step 2: Implement SigV4 `create_bucket`** (same `impl MinioFixture` block). A `PUT https://endpoint/{bucket}` with an empty body, AWS SigV4 signed (`us-east-1`, service `s3`). This is the one cryptographic helper; keep it self-contained.

```rust
    /// Create `bucket` via a SigV4-signed `PUT /{bucket}` (no `mc` binary).
    pub async fn create_bucket(&self, bucket: &str) {
        use hmac::{Hmac, Mac};
        use sha2::{Digest, Sha256};
        type HmacSha256 = Hmac<Sha256>;

        let region = "us-east-1";
        let service = "s3";
        let host = self.endpoint.strip_prefix("http://").unwrap().to_string();
        // Timestamps: YYYYMMDDtHHMMSSZ and YYYYMMDD (UTC). `time` is already a dep.
        let now = time::OffsetDateTime::now_utc();
        let amz_date = now
            .format(&time::format_description::parse("[year][month][day]T[hour][minute][second]Z").unwrap())
            .unwrap();
        let date = now
            .format(&time::format_description::parse("[year][month][day]").unwrap())
            .unwrap();

        let payload_hash = hex::encode(Sha256::digest(b""));
        let canonical_uri = format!("/{bucket}");
        let canonical_headers =
            format!("host:{host}\nx-amz-content-sha256:{payload_hash}\nx-amz-date:{amz_date}\n");
        let signed_headers = "host;x-amz-content-sha256;x-amz-date";
        let canonical_request = format!(
            "PUT\n{canonical_uri}\n\n{canonical_headers}\n{signed_headers}\n{payload_hash}"
        );
        let scope = format!("{date}/{region}/{service}/aws4_request");
        let string_to_sign = format!(
            "AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}",
            hex::encode(Sha256::digest(canonical_request.as_bytes()))
        );

        let mac = |key: &[u8], msg: &str| {
            let mut m = HmacSha256::new_from_slice(key).unwrap();
            m.update(msg.as_bytes());
            m.finalize().into_bytes()
        };
        let k_date = mac(format!("AWS4{}", self.secret_key()).as_bytes(), &date);
        let k_region = mac(&k_date, region);
        let k_service = mac(&k_region, service);
        let k_signing = mac(&k_service, "aws4_request");
        let mut sig = HmacSha256::new_from_slice(&k_signing).unwrap();
        sig.update(string_to_sign.as_bytes());
        let signature = hex::encode(sig.finalize().into_bytes());

        let authorization = format!(
            "AWS4-HMAC-SHA256 Credential={}/{scope}, SignedHeaders={signed_headers}, Signature={signature}",
            self.access_key()
        );

        let resp = reqwest::Client::new()
            .put(format!("{}/{bucket}", self.endpoint))
            .header("Host", &host)
            .header("x-amz-content-sha256", &payload_hash)
            .header("x-amz-date", &amz_date)
            .header("Authorization", authorization)
            .send()
            .await
            .expect("PUT bucket");
        // 200 = created; MinIO returns 409 BucketAlreadyOwnedByYou on re-create.
        assert!(
            resp.status().is_success() || resp.status().as_u16() == 409,
            "create_bucket failed: {}",
            resp.status()
        );
    }
}

impl Drop for MinioFixture {
    fn drop(&mut self) {
        let _ = self.server.kill();
        let _ = self.server.wait();
    }
}
```

> Implementer: `time`'s default features may not include formatting/`now_utc`. The postgres crate pins `time = "=0.3.47"`; ensure the `formatting` + `local-offset`/`std` features are available (add `features = ["formatting"]` to the `time` dep if `format()` is missing — this is additive). `now_utc()` needs no extra feature. If `time` formatting proves fiddly, format the two timestamps manually from `now.year()/.month()/...` — they are fixed-width.

- [ ] **Step 3: Confirm it compiles into the lib**

Run: `buck2 build //src/control-plane/postgres:postgres > /tmp/b.log 2>&1; grep -E "BUILD (SUCCEEDED|FAILED)|error\[" /tmp/b.log`
Expected: `BUILD SUCCEEDED`. (No standalone test here — exercised by Task 9.)

- [ ] **Step 4: Commit**

```bash
git add src/control-plane/postgres/src/fixture.rs src/control-plane/postgres/{Cargo.toml,BUCK} third-party/BUCK Cargo.lock
git commit -m "test(fixture): hermetic MinIO fixture + SigV4 bucket creation"
```

---

### Task 8: `loom_fixture_test` `minio` flag

Add an opt-in `minio = False` parameter to the macro (mirroring `duckdb`) that injects `MINIO_BIN` into the test env, so the round-trip test gets the vendored binary path.

**Files:**
- Modify: `src/control-plane/postgres/defs.bzl`

- [ ] **Step 1: Edit the macro** — add the param + env wiring (after the `duckdb` block):

```python
def loom_fixture_test(
        name,
        crate,
        srcs,
        crate_root,
        deps,
        duckdb = False,
        minio = False,
        edition = "2024",
        env = {},
        **kwargs):
    # ... existing fixture_env setup ...
    if duckdb:
        fixture_env["DUCKDB_BIN"] = "$(location //src/control-plane/postgres:duckdb-cli)"
        fixture_env["DUCKDB_EXTENSION_DIR"] = "$(location //src/control-plane/postgres:duckdb-extensions)"
    if minio:
        fixture_env["MINIO_BIN"] = "$(location //src/control-plane/postgres:minio-bin)"
    fixture_env.update(env)
    # ... native.rust_test(...) ...
```

- [ ] **Step 2: Sanity build** of an existing fixture target to ensure the macro still parses

Run: `buck2 build //src/control-plane/postgres:iceberg-writer > /tmp/b.log 2>&1; grep -E "BUILD (SUCCEEDED|FAILED)" /tmp/b.log`
Expected: `BUILD SUCCEEDED`.

- [ ] **Step 3: Commit**

```bash
git add src/control-plane/postgres/defs.bzl
git commit -m "test(buck): add minio flag to loom_fixture_test"
```

---

### Task 9: Hermetic MinIO round-trip `loom_fixture_test`

The deliverable proof: boot Postgres + MinIO, build the Iceberg catalog via `build_storage_factory` with an `s3://bucket/warehouse` URI, land/append an Iceberg table, read it back **through `DataFusionServingEngine`**, and assert (a) rows match and (b) the mirror's `data_file.path`s are `s3://…` (data physically in MinIO).

**Files:**
- Create: `src/control-plane/postgres/tests/iceberg_s3_roundtrip.rs`
- Modify: `src/control-plane/postgres/BUCK` (new `loom_fixture_test` target)

**Interfaces:**
- Consumes: `PgFixture`, `MinioFixture` (Task 7), `service_runtime::{ObjectStoreConfig, build_storage_factory, build_serving_object_store}` — **but** the round-trip test lives in the postgres crate, which must NOT depend on query-api (the serving engine lives there, and query-api depends on postgres → cycle). Resolve by asserting read-back via the catalog's **S3 `FileIO`** + the **mirror rows** + a DataFusion scan built locally in the test using `build_serving_object_store` directly (the same registration the serving engine performs), rather than importing `DataFusionServingEngine`. This proves the identical read mechanism without the dep cycle.

> Decision recorded for the reviewer: "read back through the serving engine" is honored at the *mechanism* level — the test registers the S3 object store under `s3://{bucket}` and scans the mirror's `s3://` files with DataFusion exactly as `register_iceberg_table` does. Driving the literal `DataFusionServingEngine` type would require a query-api↔postgres dependency cycle. If an end-to-end test through the actual `DataFusionServingEngine` is wanted, it belongs in `src/services/query-api/tests/` as a follow-up (would need the MinIO fixture exposed to that crate); track as a deferral.

- [ ] **Step 1: Write the round-trip test** — `tests/iceberg_s3_roundtrip.rs`. Reuse the existing `IcebergWriter`-style seeding helpers, but point the catalog at S3. Skeleton (implementer fills the seed batch using the same Arrow→append pattern as `tests/iceberg_writer.rs`):

```rust
//! Hermetic S3 round-trip: write an Iceberg table to MinIO and read it back, proving
//! the s3:// FileIO write path and the s3:// DataFusion serving read path.
use std::collections::HashMap;
use std::sync::Arc;

use control_plane_postgres::fixture::{MinioFixture, PgFixture};
use control_plane_postgres::iceberg_sql_catalog::{
    SQL_CATALOG_PROP_URI, SQL_CATALOG_PROP_WAREHOUSE, SqlCatalogBuilder,
};
use iceberg::CatalogBuilder;
use service_runtime::{ObjectStoreConfig, build_serving_object_store, build_storage_factory};
// ... arrow / iceberg writer imports mirroring tests/iceberg_writer.rs ...

#[tokio::test]
async fn s3_write_and_read_roundtrip() {
    // Fixture API mirrors tests/iceberg_writer.rs:22-28 — `start()` handles migrations
    // internally; `fresh_db()` yields (control plane, db handle); `pg_dsn(&db)` is the
    // libpq DSN for the SqlCatalog. There is no manual migration / standalone pg_dsn var.
    let fx = PgFixture::start();
    let (_cp, db) = fx.fresh_db().await;
    let pg_dsn = fx.pg_dsn(&db);
    let minio = MinioFixture::start();
    let bucket = "warehouse";
    minio.create_bucket(bucket).await;

    // Build an ObjectStoreConfig for s3://warehouse/loom against the fixture endpoint.
    let warehouse_uri = format!("s3://{bucket}/loom");
    let os_cfg = ObjectStoreConfig::for_s3_test(
        warehouse_uri.clone(),
        bucket.into(),
        minio.endpoint(),
        minio.access_key().into(),
        minio.secret_key().into(),
    );

    // Catalog over S3.
    let mut props = HashMap::new();
    props.insert(SQL_CATALOG_PROP_URI.to_string(), pg_dsn.clone());
    props.insert(SQL_CATALOG_PROP_WAREHOUSE.to_string(), warehouse_uri.clone());
    let catalog = SqlCatalogBuilder::default()
        .with_storage_factory(build_storage_factory(&os_cfg).unwrap())
        .load("loom", props)
        .await
        .unwrap();

    // ... create a table + append a known batch (e.g. 3 rows) via the iceberg writer ...

    // (a) Mirror data_file paths are s3://
    // ... query iceberg_mirror.data_file.path for the table; assert all start with "s3://" ...

    // (b) Read back via DataFusion against the registered S3 store, and assert row count.
    let (b, store) = build_serving_object_store(&os_cfg).unwrap().unwrap();
    let ctx = datafusion::prelude::SessionContext::new();
    ctx.register_object_store(
        datafusion::execution::object_store::ObjectStoreUrl::parse(format!("s3://{b}"))
            .unwrap()
            .as_ref(),
        store,
    );
    // ... register each mirror file as a PartitionedFile under s3://{bucket}, scan, count rows ...
    // assert_eq!(rows_read, 3);
}
```

- [ ] **Step 2: Add a tiny test constructor** for `ObjectStoreConfig` so the test can build one without env. In `src/services/runtime/src/lib.rs`:

```rust
impl ObjectStoreConfig {
    /// Construct an S3 config directly (test helper for fixtures that know the endpoint).
    pub fn for_s3_test(
        warehouse_uri: String,
        bucket: String,
        endpoint: String,
        access_key_id: String,
        secret_access_key: String,
    ) -> ObjectStoreConfig {
        ObjectStoreConfig {
            warehouse_uri,
            backend: ObjectStoreBackend::S3(S3Backend {
                bucket,
                endpoint: Some(endpoint),
                region: "us-east-1".into(),
                access_key_id,
                secret_access_key,
                path_style: true,
            }),
        }
    }
}
```

- [ ] **Step 3: Add the test target** — `src/control-plane/postgres/BUCK` (mirror `iceberg-writer`, add `minio = True` + the runtime/datafusion deps; note `service_runtime` is `//src/services/runtime:runtime`):

```python
loom_fixture_test(
    name = "iceberg-s3-roundtrip",
    crate = "iceberg_s3_roundtrip",
    srcs = ["tests/iceberg_s3_roundtrip.rs"],
    crate_root = "tests/iceberg_s3_roundtrip.rs",
    minio = True,
    named_deps = {"parquet57": "//third-party:parquet57"},
    deps = [
        ":postgres",
        "//src/control-plane/core:core",
        "//src/services/runtime:runtime",
        "//third-party:arrow-array",
        "//third-party:datafusion",
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

> Reconcile the dep list against the seed path you reuse: the list above mirrors `iceberg-writer` (`arrow-array` + `named_deps parquet57`). If you seed via the `iceberg_landing` Arrow-IPC pattern instead, also add `//third-party:arrow-schema` and `named_deps {"arrow_ipc57": "//third-party:arrow-ipc57"}` (the `iceberg-landing` target's deps). Add only what the chosen seed actually imports.

> Watch for a dependency cycle: `runtime` depends on `postgres`; a postgres **test** target depending on `runtime` is fine (test targets are leaves, not part of the lib's dep graph). Confirm `buck2 build` accepts it; if buck flags a cycle, move the round-trip test to `src/services/runtime/tests/` instead (runtime already deps postgres, and can dep datafusion as a dev-dep) and expose `MinioFixture` via `//src/control-plane/postgres:postgres`.

- [ ] **Step 4: Run the round-trip test**

Run: `buck2 test //src/control-plane/postgres:iceberg-s3-roundtrip > /tmp/t.log 2>&1; grep -E "FAIL|panic|Tests finished" /tmp/t.log`
Expected: `Tests finished: Pass 1. Fail 0.` (Iterate via TDD: a red run that shows the real assertion failing — e.g. paths not `s3://` or row mismatch — then fix until green.)

- [ ] **Step 5: Clippy + commit**

```bash
git add src/control-plane/postgres/tests/iceberg_s3_roundtrip.rs \
  src/control-plane/postgres/BUCK src/services/runtime/src/lib.rs
git commit -m "test(iceberg): hermetic MinIO S3 write+read round-trip"
```

---

### Task 10: Full verification + register update

**Files:**
- Modify: `docs/ROADMAP.md` (close `road-iceberg-real-object-store`), `docs/FUTURE.md` (record deferrals)

- [ ] **Step 1: Full build + test sweep** (the shared-dep regression guard — object_store feature union touched the whole graph)

Run: `buck2 build //src/... > /tmp/b.log 2>&1; grep -E "BUILD (SUCCEEDED|FAILED)" /tmp/b.log`
Run: `buck2 test //src/... > /tmp/t.log 2>&1; grep -E "FAIL|Tests finished" /tmp/t.log`
Expected: build succeeds; all tests pass. If any unrelated `duckdb`/DuckLake fixture test fails, it's the known reindeer-update downgrade footgun — `cargo update -p duckdb --precise 1.10503.1 && ./tools/buckify.sh` (see CLAUDE.md).

- [ ] **Step 2: Lint hooks**

Run: `buck2 run //tools:prek -- run --all-files > /tmp/l.log 2>&1; grep -E "Failed|Passed|error" /tmp/l.log`
Expected: all hooks pass (commit any files the hooks rewrite).

- [ ] **Step 3: Close the register item** via `loom-docs-update`. In `docs/ROADMAP.md`, flip `road-iceberg-real-object-store` to `- [x]`, set `status:done`, add `pr:#<N>` (after the PR is opened). Record deferrals in `docs/FUTURE.md` (new items, `status:deferred`, linking `[[road-iceberg-real-object-store]]`):
  - S3 multipart upload for large data files (the buffered `S3FileWrite` PUTs whole files).
  - DuckLake object-store S3 routing (this slice is Iceberg-only).
  - End-to-end serving read through the literal `DataFusionServingEngine` against S3 (the round-trip proves the mechanism, not the type).
  - Helm/deploy S3 env wiring (`[[fut-deploy-followups]]`).

- [ ] **Step 4: Commit docs**

```bash
git add docs/ROADMAP.md docs/FUTURE.md
git commit -m "docs(registers): close road-iceberg-real-object-store; record S3 deferrals"
```

---

## Self-Review

**1. Spec coverage:**
- §1 Config surface (`LOOM_WAREHOUSE_URI` scheme-select, `AWS_*`, path-style-on-endpoint, fail-fast on missing creds, file:// default) → Task 2. ✅
- §2 Factory seam (`build_storage_factory`, file://→LocalFs, s3://→S3StorageFactory; mains stop hardcoding) → Tasks 1, 3, 4. ✅ Resolves the spec's "open implementation question": iceberg 0.9 ships **no** S3 storage, so we add a custom `S3StorageFactory` over `object_store` (the spec's second offered branch). ✅
- §3 Proof (vendor MinIO, fixture, bucket via SigV4 PUT not `mc`, round-trip asserting rows + `s3://` paths) → Tasks 6, 7, 8, 9. ✅
- **Deviation from spec (documented):** spec §2 says "No change to the serving path" — false; the DataFusion read hardwires local FS. Task 5 makes it S3-aware (user-approved scope expansion). The round-trip reads back via the same registration mechanism (dep-cycle avoidance noted in Task 9). ✅
- Out-of-scope items (DuckLake S3, real-AWS CI, Helm, advanced creds, multipart tuning) honored — none implemented; relevant ones recorded as deferrals (Task 10). ✅

**2. Placeholder scan:** The only deliberately-deferred concrete values are the two MinIO `sha256`s (Task 6 Step 1 gives the exact command to compute them) and the PR number in the register close (Task 10, known only after PR open). No "TODO/handle errors/similar to" placeholders; all code blocks are complete.

**3. Type consistency:** `S3StorageFactory::new(bucket, endpoint: Option<String>, region, access_key_id, secret_access_key, path_style)` is identical in Tasks 1 and 3. `ObjectStoreBackend`/`S3Backend`/`ObjectStoreConfig` field names match across Tasks 2, 3, 9. `build_serving_object_store -> Option<(String, Arc<dyn ObjectStore>)>` matches its consumers in Tasks 3, 5, 9. `DataFusionServingEngine::new(catalog, serving_store)` arity matches Task 5 Step 5's call site. `register_iceberg_table(ctx, catalog, table, serving_store)` matches its call site update. `object_store_url_for` / `key_of` are each defined once and used consistently.
