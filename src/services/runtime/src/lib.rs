//! Shared service runtime: parse config from the environment, build a Postgres-backed
//! control plane + object store, and serve an axum router. Used by the ingest and
//! query-api binaries so each `main` stays thin.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
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
}

/// Fully-resolved service configuration.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Config {
    pub bind_addr: SocketAddr,
    pub db: DbConfig,
    pub data_path: PathBuf,
    pub lock_timeout: Duration,
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

        Ok(Config {
            bind_addr,
            db: DbConfig {
                host: req("LOOM_DB_HOST")?,
                port,
                user: req("LOOM_DB_USER")?,
                password: req("LOOM_DB_PASSWORD")?,
                dbname: req("LOOM_DB_NAME")?,
            },
            data_path: PathBuf::from(req("LOOM_DATA_PATH")?),
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
