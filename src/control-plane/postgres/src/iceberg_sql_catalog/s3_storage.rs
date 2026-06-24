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
use object_store::aws::{AmazonS3, AmazonS3Builder};
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
    pub fn key_of(path: &str) -> Result<ObjPath> {
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
