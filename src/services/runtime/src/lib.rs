//! Shared service runtime: parse config from the environment, build a Postgres-backed
//! control plane + object store, and serve an axum router. Used by the ingest and
//! query-api binaries so each `main` stays thin.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use control_plane_postgres::PgControlPlane;
use object_store::local::LocalFileSystem;
use sqlx::PgPool;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};

/// Discrete Postgres connection fields. Feeds both the sqlx control-plane pool and
/// DuckLake's ATTACH connection string, with no URL parsing in between.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DbConfig {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub dbname: String,
}

impl DbConfig {
    /// sqlx connect options. A `host` beginning with `/` is a unix-socket directory
    /// (libpq convention); otherwise a TCP host:port.
    pub fn pg_connect_options(&self) -> PgConnectOptions {
        let base = if self.host.starts_with('/') {
            PgConnectOptions::new().socket(&self.host)
        } else {
            PgConnectOptions::new().host(&self.host).port(self.port)
        };
        base.username(&self.user)
            .password(&self.password)
            .database(&self.dbname)
    }

    /// libpq-style connection string for `ATTACH 'ducklake:postgres:<...>'`.
    pub fn ducklake_libpq(&self) -> String {
        format!(
            "dbname={} host={} port={} user={} password={}",
            self.dbname, self.host, self.port, self.user, self.password
        )
    }

    /// A sqlx-connectable `postgres://` URL — what the vendored Iceberg SQL catalog
    /// (`SqlCatalog`) opens its own pool with. A `host` beginning with `/` is a unix
    /// socket directory (passed as a `?host=` query param, libpq convention, with the
    /// authority host left as `localhost`); otherwise a TCP `host:port` authority.
    ///
    /// NOTE: `user`/`password` are interpolated raw, not percent-encoded — a password
    /// containing URL-reserved characters (`@ : / ? #`) would corrupt parsing. This is
    /// the only connection form with that limitation (`ducklake_libpq` is libpq
    /// space-delimited; `pg_connect_options` is structured). Acceptable for the current
    /// controlled-deploy posture; percent-encode here if free-form passwords are ever
    /// supported.
    pub fn pg_url(&self) -> String {
        if self.host.starts_with('/') {
            format!(
                "postgres://{}:{}@localhost/{}?host={}",
                self.user, self.password, self.dbname, self.host
            )
        } else {
            format!(
                "postgres://{}:{}@{}:{}/{}",
                self.user, self.password, self.host, self.port, self.dbname
            )
        }
    }
}

/// Fully-resolved service configuration.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Config {
    pub bind_addr: SocketAddr,
    pub db: DbConfig,
    pub data_path: PathBuf,
    pub object_store: ObjectStoreConfig,
    pub lock_timeout: Duration,
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("missing required environment variable: {0}")]
    MissingVar(String),
    #[error("invalid value for {var}: {detail}")]
    Invalid { var: String, detail: String },
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

impl Config {
    /// Parse from a key->value map. `from_env` wraps this with `std::env::vars()`.
    pub fn from_map(vars: &HashMap<String, String>) -> Result<Config, ConfigError> {
        let req = |k: &str| {
            vars.get(k)
                .cloned()
                .ok_or_else(|| ConfigError::MissingVar(k.to_string()))
        };
        let invalid = |var: &str, detail: String| ConfigError::Invalid {
            var: var.to_string(),
            detail,
        };

        let bind_addr = req("LOOM_BIND_ADDR")?
            .parse()
            .map_err(|e: std::net::AddrParseError| invalid("LOOM_BIND_ADDR", e.to_string()))?;
        let port = req("LOOM_DB_PORT")?
            .parse::<u16>()
            .map_err(|e| invalid("LOOM_DB_PORT", e.to_string()))?;
        let lock_timeout = match vars.get("LOOM_LOCK_TIMEOUT_MS") {
            Some(s) => Duration::from_millis(
                s.parse::<u64>()
                    .map_err(|e| invalid("LOOM_LOCK_TIMEOUT_MS", e.to_string()))?,
            ),
            None => Duration::from_millis(5000),
        };

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
    }

    /// Read the config keys from the process environment.
    pub fn from_env() -> Result<Config, ConfigError> {
        let vars: HashMap<String, String> = std::env::vars().collect();
        Self::from_map(&vars)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum RuntimeError {
    #[error("connect pool: {0}")]
    Pool(sqlx::Error),
    #[error("object store: {0}")]
    Store(object_store::Error),
    #[error("bind {0}")]
    Bind(std::io::Error),
    #[error("serve: {0}")]
    Serve(std::io::Error),
}

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

/// Connect a control-plane pool from the DB config.
pub async fn build_pool(db: &DbConfig) -> Result<PgPool, RuntimeError> {
    PgPoolOptions::new()
        .connect_with(db.pg_connect_options())
        .await
        .map_err(RuntimeError::Pool)
}

/// Wrap a pool as a `PgControlPlane`.
pub fn control_plane(pool: PgPool, lock_timeout: Duration) -> PgControlPlane {
    PgControlPlane::new(pool, lock_timeout)
}

/// A `LocalFileSystem` object store rooted at `data_path`.
pub fn local_store(data_path: &Path) -> Result<LocalFileSystem, RuntimeError> {
    LocalFileSystem::new_with_prefix(data_path).map_err(RuntimeError::Store)
}

/// Bind `bind_addr` and serve `router` until the process is terminated.
pub async fn serve(bind_addr: SocketAddr, router: Router) -> Result<(), RuntimeError> {
    let listener = tokio::net::TcpListener::bind(bind_addr)
        .await
        .map_err(RuntimeError::Bind)?;
    axum::serve(listener, router)
        .await
        .map_err(RuntimeError::Serve)
}

/// Install a `tracing-subscriber` for the process. Uses `RUST_LOG` env (default
/// `info`). Idempotent — a second call from a test harness or re-entrant path does
/// not panic.
pub fn init_tracing() {
    use tracing_subscriber::{EnvFilter, fmt};
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let _ = fmt().with_env_filter(filter).try_init();
}
