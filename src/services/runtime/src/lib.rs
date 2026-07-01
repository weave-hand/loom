//! Shared service runtime: parse config from the environment, build a Postgres-backed
//! control plane + object store, and serve an axum router. Used by the ingest and
//! query-api binaries so each `main` stays thin.

mod auth;
pub use auth::{
    AuthState, BootstrapError, Subject, bootstrap_admin, login_routes, protect, require_auth,
    session_routes, session_ttl_from_env, status_for,
};

mod admin;
pub use admin::{AdminState, admin_routes, require_admin};

mod crypto;
pub use crypto::{AuthError, generate_session_token, hash_password, token_sha256, verify_password};

mod openapi;
pub use openapi::{BEARER_SCHEME_NAME, with_openapi};

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

pub use loom_config::{
    ConfigError, LayeredConfig, env_map, invalid, load, overlay_opt, parse_config_doc,
};

/// Embedded-Postgres settings, present only when `LOOM_PG_MODE=embedded`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EmbeddedSettings {
    pub cfg: managed_postgres::EmbeddedPgConfig,
}

/// Discrete Postgres connection fields. Feeds the sqlx control-plane pool and the
/// Iceberg SQL catalog, with no URL parsing in between.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DbConfig {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub dbname: String,
    /// Max pool connections. `None` ⇒ sqlx default. From `LOOM_DB_MAX_CONNECTIONS`.
    pub max_connections: Option<u32>,
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

    /// A sqlx-connectable `postgres://` URL — what the vendored Iceberg SQL catalog
    /// (`SqlCatalog`) opens its own pool with. A `host` beginning with `/` is a unix
    /// socket directory (passed as a `?host=` query param, libpq convention, with the
    /// authority host left as `localhost`); otherwise a TCP `host:port` authority.
    ///
    /// NOTE: `user`/`password` are interpolated raw, not percent-encoded — a password
    /// containing URL-reserved characters (`@ : / ? #`) would corrupt parsing. This is
    /// the only URL-form connection (unlike `pg_connect_options`, which is structured).
    /// Acceptable for the current controlled-deploy posture; percent-encode here if
    /// free-form passwords are ever supported.
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
    /// Present when running an embedded (loom-managed) Postgres cluster.
    pub embedded: Option<EmbeddedSettings>,
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

        let max_connections = match vars.get("LOOM_DB_MAX_CONNECTIONS") {
            Some(s) => Some(
                s.parse::<u32>()
                    .map_err(|e| invalid("LOOM_DB_MAX_CONNECTIONS", e.to_string()))?,
            ),
            None => None,
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

        let embedded = if vars.get("LOOM_PG_MODE").map(String::as_str) == Some("embedded") {
            let bin_dir = PathBuf::from(req("LOOM_PG_BIN_DIR")?);
            Some(EmbeddedSettings {
                cfg: managed_postgres::EmbeddedPgConfig {
                    bin_dir,
                    ld_library_path: vars
                        .get("LOOM_PG_LD_LIBRARY_PATH")
                        .cloned()
                        .unwrap_or_default(),
                    data_dir: data_path.join("pgdata"),
                    socket_dir: data_path.join("pgrun"),
                    database: req("LOOM_DB_NAME")?,
                },
            })
        } else {
            None
        };

        Ok(Config {
            bind_addr,
            db: DbConfig {
                host: req("LOOM_DB_HOST")?,
                port,
                user: req("LOOM_DB_USER")?,
                password: req("LOOM_DB_PASSWORD")?,
                dbname: req("LOOM_DB_NAME")?,
                max_connections,
            },
            data_path,
            object_store,
            lock_timeout,
            gc_retention,
            embedded,
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
    #[error("embedded postgres: {0}")]
    Embedded(managed_postgres::EmbeddedPgError),
    #[error("migrate: {0}")]
    Migrate(control_plane_core::ControlPlaneError),
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
    let mut opts = PgPoolOptions::new();
    if let Some(n) = db.max_connections {
        opts = opts.max_connections(n);
    }
    opts.connect_with(db.pg_connect_options())
        .await
        .map_err(RuntimeError::Pool)
}

/// Build a control-plane pool, owning an embedded Postgres cluster when configured.
/// External mode is identical to `build_pool` and returns `None`. In embedded mode
/// the returned `EmbeddedPg` must be kept alive for the process lifetime and
/// `shutdown()` on exit.
pub async fn build_pool_managed(
    cfg: &Config,
) -> Result<(PgPool, Option<managed_postgres::EmbeddedPg>), RuntimeError> {
    match &cfg.embedded {
        None => Ok((build_pool(&cfg.db).await?, None)),
        Some(e) => {
            let pg = managed_postgres::EmbeddedPg::start(e.cfg.clone())
                .await
                .map_err(RuntimeError::Embedded)?;
            let mut opts = PgPoolOptions::new();
            if let Some(n) = cfg.db.max_connections {
                opts = opts.max_connections(n);
            }
            let pool = opts
                .connect_with(pg.connect_options())
                .await
                .map_err(RuntimeError::Pool)?;
            control_plane_postgres::run_embedded_migrations(&pool)
                .await
                .map_err(RuntimeError::Migrate)?;
            Ok((pool, Some(pg)))
        }
    }
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
    drop(fmt().with_env_filter(filter).try_init());
}
