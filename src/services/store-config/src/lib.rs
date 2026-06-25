//! Postgres-free object-store configuration extracted from `service_runtime`.
//! Provides `ObjectStoreConfig`, the per-backend parse logic, the serving store
//! builder, and the writable `WriteStore` type — all without any Postgres dependency
//! so the zero-pool transform worker can depend on this crate directly.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use object_store::ObjectStore;
use object_store::local::LocalFileSystem;

/// Parse / build errors for object-store config.
#[derive(Debug, thiserror::Error)]
pub enum StoreConfigError {
    #[error("missing required environment variable: {0}")]
    Missing(String),
    #[error("invalid value for {var}: {detail}")]
    Invalid { var: String, detail: String },
    #[error("object store: {0}")]
    Store(object_store::Error),
}

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

    /// Parse from the env map. `data_path` is the back-compat default warehouse root
    /// used when `LOOM_WAREHOUSE_URI` is unset.
    pub fn parse(
        vars: &HashMap<String, String>,
        data_path: &Path,
    ) -> Result<ObjectStoreConfig, StoreConfigError> {
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
                .ok_or_else(|| StoreConfigError::Invalid {
                    var: "LOOM_WAREHOUSE_URI".into(),
                    detail: "s3:// URI must include a bucket (s3://bucket/prefix)".into(),
                })?
                .to_string();
            let req = |k: &str| {
                vars.get(k)
                    .cloned()
                    .filter(|v| !v.is_empty())
                    .ok_or_else(|| StoreConfigError::Missing(k.to_string()))
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
            return Err(StoreConfigError::Invalid {
                var: "LOOM_WAREHOUSE_URI".into(),
                detail: format!("unsupported scheme in {warehouse_uri}; use file:// or s3://"),
            });
        };
        Ok(ObjectStoreConfig {
            warehouse_uri,
            backend,
        })
    }

    /// Parse from env without a `data_path` fallback: `LOOM_WAREHOUSE_URI` is
    /// required (postgres-free callers like the zero-pool worker have no data dir).
    pub fn parse_from_env(vars: &HashMap<String, String>) -> Result<Self, StoreConfigError> {
        let uri = vars
            .get("LOOM_WAREHOUSE_URI")
            .ok_or_else(|| StoreConfigError::Missing("LOOM_WAREHOUSE_URI".into()))?;
        // Reuse the real parse with the warehouse path as its own data_path: when
        // LOOM_WAREHOUSE_URI is present, `parse` ignores the data_path fallback.
        let stripped = uri.strip_prefix("file://").unwrap_or(uri);
        Self::parse(vars, Path::new(stripped))
    }
}

/// A `LocalFileSystem` object store rooted at `data_path`.
pub fn local_store(data_path: &Path) -> Result<LocalFileSystem, StoreConfigError> {
    LocalFileSystem::new_with_prefix(data_path).map_err(StoreConfigError::Store)
}

/// Bucket name + object store handle returned by [`build_serving_object_store`].
pub type ServingStore = (String, Arc<dyn ObjectStore>);

/// Build the DataFusion serving read store for `s3://` warehouses. Returns the bucket
/// name (for the `ObjectStoreUrl`) + the store, or `None` for local-filesystem reads.
pub fn build_serving_object_store(
    cfg: &ObjectStoreConfig,
) -> Result<Option<ServingStore>, StoreConfigError> {
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
            let store = b.build().map_err(StoreConfigError::Store)?;
            Ok(Some((s.bucket.clone(), Arc::new(store))))
        }
    }
}

/// A writable object store plus the absolute URL root under which its keys live, so
/// callers can form absolute data-file paths (`{root_url}/{schema}/{table}/{rel}`).
pub struct WriteStore {
    pub store: Arc<dyn ObjectStore>,
    pub root_url: String,
}

/// Build a writable object store for writing coalesced Parquet files.
pub fn build_write_store(cfg: &ObjectStoreConfig) -> Result<WriteStore, StoreConfigError> {
    match &cfg.backend {
        ObjectStoreBackend::Local => {
            // warehouse_uri is "file://<abs>"; LocalFileSystem roots at the abs path.
            let path = cfg
                .warehouse_uri
                .strip_prefix("file://")
                .unwrap_or(&cfg.warehouse_uri);
            Ok(WriteStore {
                store: Arc::new(local_store(Path::new(path))?),
                root_url: cfg.warehouse_uri.clone(),
            })
        }
        ObjectStoreBackend::S3(_) => {
            let (bucket, store) =
                build_serving_object_store(cfg)?.expect("S3 backend yields a serving store");
            Ok(WriteStore {
                store,
                root_url: format!("s3://{bucket}"),
            })
        }
    }
}
