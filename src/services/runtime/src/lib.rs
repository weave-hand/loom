//! Shared service runtime: parse config from the environment, build a Postgres-backed
//! control plane + object store, and serve an axum router. Used by the ingest and
//! query-api binaries so each `main` stays thin.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use control_plane_postgres::PgControlPlane;
use sqlx::PgPool;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};

// Re-export the postgres-free object-store types so engine/ingest callers see them
// at `service_runtime::ObjectStoreConfig` etc. (unchanged import paths).
pub use store_config::{
    ObjectStoreBackend, ObjectStoreConfig, S3Backend, ServingStore, WriteStore,
    build_serving_object_store, build_write_store, local_store,
};

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
    /// Retention window for physical GC of end-capped Iceberg rows. Snapshots
    /// older than this are eligible for reclamation. From `LOOM_GC_RETENTION_SECS`
    /// (default 7 days). The `_SECS` unit suffix matches `LOOM_LOCK_TIMEOUT_MS`.
    pub gc_retention: Duration,
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("missing required environment variable: {0}")]
    MissingVar(String),
    #[error("invalid value for {var}: {detail}")]
    Invalid { var: String, detail: String },
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
        let gc_retention = match vars.get("LOOM_GC_RETENTION_SECS") {
            Some(s) => Duration::from_secs(
                s.parse::<u64>()
                    .map_err(|e| invalid("LOOM_GC_RETENTION_SECS", e.to_string()))?,
            ),
            None => Duration::from_secs(7 * 24 * 3600),
        };

        let data_path = PathBuf::from(req("LOOM_DATA_PATH")?);
        let object_store = ObjectStoreConfig::parse(vars, &data_path).map_err(|e| match e {
            store_config::StoreConfigError::Missing(k) => ConfigError::MissingVar(k),
            store_config::StoreConfigError::Invalid { var, detail } => {
                ConfigError::Invalid { var, detail }
            }
            store_config::StoreConfigError::Store(inner) => ConfigError::Invalid {
                var: "LOOM_WAREHOUSE_URI".into(),
                detail: inner.to_string(),
            },
        })?;

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
            gc_retention,
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
