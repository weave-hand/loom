//! Shared service runtime: parse config from the environment, build a Postgres-backed
//! control plane + object store, and serve an axum router. Used by the ingest and
//! query-api binaries so each `main` stays thin.

mod auth;
pub use auth::{
    AuthState, Subject, auth_openapi, login_lockout, login_routes, protect, require_auth,
    resolve_bearer, service_account_openapi, service_account_routes, service_token_max_ttl,
    session_routes, session_ttl, status_for,
};
pub use control_plane_core::LockoutPolicy;

pub mod create_admin;

mod admin;
pub use admin::{AdminState, admin_openapi, admin_routes, require_admin};

mod crypto;
pub use crypto::{AuthError, generate_session_token, hash_password, token_sha256, verify_password};

mod openapi;
pub use openapi::{
    BEARER_SCHEME_NAME, register_bearer_scheme, with_openapi, with_openapi_provider,
};

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
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
    ConfigError, LayeredConfig, env_map, invalid, load, overlay_opt, parse_config_doc, parse_var,
    req_var,
};

/// Default application database name in embedded mode (used by both
/// `DbConfig::from_map` and `EmbeddedSettings::from_map` so `cfg.db` and the
/// embedded cluster's database agree by construction).
const DEFAULT_EMBEDDED_DB_NAME: &str = "loom";

/// The PG-binary paths needed only to *boot* an embedded cluster. Absent for
/// processes that merely connect (e.g. `loom create-admin`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PgBinPaths {
    /// Postgres `bin/` directory (holds initdb, postgres, pg_ctl).
    pub bin_dir: PathBuf,
    /// `LD_LIBRARY_PATH` for the spawned binaries.
    pub ld_library_path: String,
}

/// Embedded-Postgres settings, present only when `LOOM_PG_MODE=embedded`. The
/// data/socket dirs and database name derive from `data_path` + the (defaulted)
/// DB name; `bin` is populated only when `LOOM_PG_BIN_DIR` is supplied — the
/// spawn site (`build_pool_managed`) requires it, parse time does not.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EmbeddedSettings {
    /// Persistent cluster data dir (`<data_path>/pgdata`).
    pub data_dir: PathBuf,
    /// Unix-socket directory (`<data_path>/pgrun`).
    pub socket_dir: PathBuf,
    /// Application database name (defaults to `loom` in embedded mode).
    pub database: String,
    /// PG-binary paths; `None` when no cluster is booted in-process.
    pub bin: Option<PgBinPaths>,
}

impl EmbeddedSettings {
    /// Parse the embedded-PG settings from the env snapshot: `Some` only when
    /// `LOOM_PG_MODE=embedded`. `LOOM_PG_BIN_DIR` is never required here — it is
    /// captured into `bin` when present and left `None` otherwise. The database
    /// name uses the same embedded default as `DbConfig::from_map` so the two are
    /// consistent by construction.
    pub fn from_map(
        vars: &HashMap<String, String>,
        data_path: &Path,
    ) -> Result<Option<EmbeddedSettings>, ConfigError> {
        if vars.get("LOOM_PG_MODE").map(String::as_str) != Some("embedded") {
            return Ok(None);
        }
        let bin = vars.get("LOOM_PG_BIN_DIR").map(|dir| PgBinPaths {
            bin_dir: PathBuf::from(dir),
            ld_library_path: vars
                .get("LOOM_PG_LD_LIBRARY_PATH")
                .cloned()
                .unwrap_or_default(),
        });
        Ok(Some(EmbeddedSettings {
            data_dir: data_path.join("pgdata"),
            socket_dir: data_path.join("pgrun"),
            database: vars
                .get("LOOM_DB_NAME")
                .cloned()
                .unwrap_or_else(|| DEFAULT_EMBEDDED_DB_NAME.to_string()),
            bin,
        }))
    }
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
    /// Parse the discrete `LOOM_DB_*` connection fields from the env snapshot.
    /// In embedded mode (`LOOM_PG_MODE=embedded`) the five vars default to values
    /// consistent with `EmbeddedPg::connect_options()` (socket under
    /// `<LOOM_DATA_PATH>/pgrun`, user `postgres`, trust auth, db `loom`); explicit
    /// vars still override. In external mode all five stay required.
    pub fn from_map(vars: &HashMap<String, String>) -> Result<DbConfig, ConfigError> {
        if vars.get("LOOM_PG_MODE").map(String::as_str) == Some("embedded") {
            Self::from_map_embedded(vars)
        } else {
            Self::from_map_external(vars)
        }
    }

    /// Embedded-mode fields: the five `LOOM_DB_*` vars default to values consistent
    /// with `EmbeddedPg::connect_options()` (socket `<LOOM_DATA_PATH>/pgrun`, port
    /// 5432, user `postgres`, trust password, db `loom`); explicit vars override.
    fn from_map_embedded(vars: &HashMap<String, String>) -> Result<DbConfig, ConfigError> {
        // Socket dir matches EmbeddedSettings' `<data_path>/pgrun`. LOOM_DATA_PATH is
        // required by Config::from_map before this runs, so req_var is safe.
        let data_path = PathBuf::from(req_var(vars, "LOOM_DATA_PATH")?);
        let default_host = data_path.join("pgrun").display().to_string();
        Ok(DbConfig {
            host: vars.get("LOOM_DB_HOST").cloned().unwrap_or(default_host),
            port: match vars.get("LOOM_DB_PORT") {
                Some(s) => s.parse::<u16>().map_err(|e| invalid("LOOM_DB_PORT", e))?,
                None => 5432,
            },
            user: vars
                .get("LOOM_DB_USER")
                .cloned()
                .unwrap_or_else(|| "postgres".to_string()),
            password: vars.get("LOOM_DB_PASSWORD").cloned().unwrap_or_default(),
            dbname: vars
                .get("LOOM_DB_NAME")
                .cloned()
                .unwrap_or_else(|| DEFAULT_EMBEDDED_DB_NAME.to_string()),
            max_connections: Self::max_connections(vars)?,
        })
    }

    /// External-mode fields: all five `LOOM_DB_*` vars are required.
    fn from_map_external(vars: &HashMap<String, String>) -> Result<DbConfig, ConfigError> {
        Ok(DbConfig {
            host: req_var(vars, "LOOM_DB_HOST")?,
            port: req_var(vars, "LOOM_DB_PORT")?
                .parse::<u16>()
                .map_err(|e| invalid("LOOM_DB_PORT", e))?,
            user: req_var(vars, "LOOM_DB_USER")?,
            password: req_var(vars, "LOOM_DB_PASSWORD")?,
            dbname: req_var(vars, "LOOM_DB_NAME")?,
            max_connections: Self::max_connections(vars)?,
        })
    }

    /// Parse the optional `LOOM_DB_MAX_CONNECTIONS` (shared by both modes).
    fn max_connections(vars: &HashMap<String, String>) -> Result<Option<u32>, ConfigError> {
        match vars.get("LOOM_DB_MAX_CONNECTIONS") {
            Some(s) => Ok(Some(
                s.parse::<u32>()
                    .map_err(|e| invalid("LOOM_DB_MAX_CONNECTIONS", e))?,
            )),
            None => Ok(None),
        }
    }

    /// sqlx connect options. A `host` beginning with `/` is a unix-socket directory
    /// (libpq convention); otherwise a TCP host:port.
    pub fn pg_connect_options(&self) -> PgConnectOptions {
        let base = if self.host.starts_with('/') {
            PgConnectOptions::new().socket(&self.host)
        } else {
            PgConnectOptions::new().host(&self.host)
        };
        // `.port()` applies to both branches: for a unix socket it selects the
        // `.s.PGSQL.<port>` socket file, so it must be set even in socket mode or
        // sqlx probes the default 5432 and misses a cluster on any other port.
        base.port(self.port)
            .username(&self.user)
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
            // The port is carried in the authority even for the socket form: libpq/sqlx
            // derive the `.s.PGSQL.<port>` socket filename from it, so omitting it probes
            // the default 5432 and misses a cluster listening on any other port.
            format!(
                "postgres://{}:{}@localhost:{}/{}?host={}",
                self.user, self.password, self.port, self.dbname, self.host
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
    /// Grace window for the orphaned-object sweep: an unreferenced warehouse
    /// object younger than this is held (guards the write-then-commit race). From
    /// `LOOM_ORPHAN_SWEEP_GRACE_SECS` (default 24h). Mirrors `gc_retention`'s
    /// `_SECS` unit convention.
    pub orphan_sweep_grace: Duration,
    /// Present when running an embedded (loom-managed) Postgres cluster.
    pub embedded: Option<EmbeddedSettings>,
    /// When `true`, `build_pool_managed`'s external branch applies the embedded
    /// control-plane migrations after connecting. From `LOOM_DB_MIGRATE_ON_BOOT`
    /// (default `false`). The embedded branch always migrates regardless.
    pub migrate_on_boot: bool,
}

/// Parse `LOOM_DB_MIGRATE_ON_BOOT` (default `false`). Only the literal
/// `true`/`false` are accepted; anything else fails startup naming the key.
pub fn parse_migrate_on_boot(vars: &HashMap<String, String>) -> Result<bool, ConfigError> {
    match vars.get("LOOM_DB_MIGRATE_ON_BOOT").map(String::as_str) {
        None | Some("false") => Ok(false),
        Some("true") => Ok(true),
        Some(other) => Err(invalid(
            "LOOM_DB_MIGRATE_ON_BOOT",
            format!("expected `true` or `false`, got `{other}`"),
        )),
    }
}

impl Config {
    /// Parse from a key->value map. `from_env` wraps this with `std::env::vars()`.
    pub fn from_map(vars: &HashMap<String, String>) -> Result<Config, ConfigError> {
        let bind_addr = req_var(vars, "LOOM_BIND_ADDR")?
            .parse()
            .map_err(|e: std::net::AddrParseError| invalid("LOOM_BIND_ADDR", e))?;
        let lock_timeout =
            Duration::from_millis(parse_var(vars, "LOOM_LOCK_TIMEOUT_MS", 5000_u64)?);
        let gc_retention = Duration::from_secs(parse_var(
            vars,
            "LOOM_GC_RETENTION_SECS",
            7 * 24 * 3600_u64,
        )?);
        let orphan_sweep_grace = Duration::from_secs(parse_var(
            vars,
            "LOOM_ORPHAN_SWEEP_GRACE_SECS",
            24 * 3600_u64,
        )?);
        let data_path = PathBuf::from(req_var(vars, "LOOM_DATA_PATH")?);
        let object_store = ObjectStoreConfig::parse(vars, &data_path)?;
        Ok(Config {
            bind_addr,
            db: DbConfig::from_map(vars)?,
            data_path: data_path.clone(),
            object_store,
            lock_timeout,
            gc_retention,
            orphan_sweep_grace,
            embedded: EmbeddedSettings::from_map(vars, &data_path)?,
            migrate_on_boot: parse_migrate_on_boot(vars)?,
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
    #[error("config: {0}")]
    Config(#[from] ConfigError),
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
        None => {
            let pool = build_pool(&cfg.db).await?;
            if cfg.migrate_on_boot {
                tracing::info!("LOOM_DB_MIGRATE_ON_BOOT=true: applying control-plane migrations");
                control_plane_postgres::run_embedded_migrations(&pool)
                    .await
                    .map_err(RuntimeError::Migrate)?;
            }
            Ok((pool, None))
        }
        Some(e) => {
            let bin = e.bin.as_ref().ok_or_else(|| {
                RuntimeError::Config(ConfigError::Invalid {
                    var: "LOOM_PG_BIN_DIR".to_string(),
                    detail: "required to boot the embedded Postgres cluster; client-only \
                             tools that merely connect do not need it"
                        .to_string(),
                })
            })?;
            let pg = managed_postgres::EmbeddedPg::start(managed_postgres::EmbeddedPgConfig {
                bin_dir: bin.bin_dir.clone(),
                ld_library_path: bin.ld_library_path.clone(),
                data_dir: e.data_dir.clone(),
                socket_dir: e.socket_dir.clone(),
                database: e.database.clone(),
            })
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

/// `true` when the process was started in migrate-and-exit mode
/// (`LOOM_MIGRATE=apply`, read from the caller's env snapshot). [`bootstrap`]
/// checks this before normal startup — and the standalone main before its
/// embedded self-extract — so a chart hook Job can run any service image as a
/// one-shot migrator.
pub fn migrate_requested(vars: &HashMap<String, String>) -> bool {
    vars.get("LOOM_MIGRATE").map(String::as_str) == Some("apply")
}

/// Outcome of [`bootstrap`]: migrate-and-exit mode completed, or the full
/// service context is ready to serve.
#[expect(
    clippy::large_enum_variant,
    reason = "one value per process at startup; boxing buys nothing"
)]
pub enum Boot {
    /// `LOOM_MIGRATE=apply`: migrations applied; the caller should exit 0.
    Migrated,
    /// Normal startup: everything a service main needs to serve.
    Ready(ServiceContext),
}

/// The shared startup product: config, control-plane pool, concrete
/// `PgControlPlane`, auth state, and the service-token TTL cap. Owns the
/// embedded-PG handle so the keep-alive is structural — the cluster lives
/// exactly as long as the context, replacing the per-main `_pg` binding every
/// caller had to remember. Mains deliberately do NOT gracefully stop the
/// embedded cluster (parity with the previous behavior); the standalone
/// composite keeps its own `build_pool_managed` + `stop_pg` for the graceful
/// path and does not use `bootstrap`.
#[expect(
    clippy::partial_pub_fields,
    reason = "_embedded is a keep-alive guard, not API — private so callers cannot detach the cluster's lifetime from the context"
)]
pub struct ServiceContext {
    pub cfg: Config,
    pub pool: PgPool,
    pub pg: Arc<PgControlPlane>,
    pub auth: AuthState,
    pub max_ttl: Duration,
    _embedded: Option<managed_postgres::EmbeddedPg>,
}

/// One startup path for the service mains: parse config from the env snapshot,
/// honor migrate-and-exit mode, then build pool → control plane → auth. The
/// fail-loud TTL reads happen BEFORE the pool boots (a typo'd TTL should not
/// cost an embedded initdb). The full config is parsed before the migrate gate,
/// exactly as every main did.
pub async fn bootstrap(vars: &HashMap<String, String>) -> Result<Boot, RuntimeError> {
    let cfg = Config::from_map(vars)?;
    if migrate_requested(vars) {
        run_migrations(&cfg.db).await?;
        return Ok(Boot::Migrated);
    }
    let session_ttl = auth::session_ttl(vars)?;
    let lockout = auth::login_lockout(vars)?;
    let max_ttl = auth::service_token_max_ttl(vars)?;
    let (pool, embedded) = build_pool_managed(&cfg).await?;
    let pg = Arc::new(control_plane(pool.clone(), cfg.lock_timeout));
    let auth = AuthState {
        auth: pg.clone(),
        session_ttl,
        lockout,
    };
    Ok(Boot::Ready(ServiceContext {
        cfg,
        pool,
        pg,
        auth,
        max_ttl,
        _embedded: embedded,
    }))
}

/// Connect an external control-plane pool from `db` and apply the embedded
/// migrations. Used by the migrate-and-exit entrypoint (see [`migrate_requested`]).
///
/// The migrate Job can start before a freshly-provisioned Postgres (e.g. the
/// chart's bundled CloudNativePG on a first `helm install`) is accepting
/// connections, so the initial connect is retried for up to ~2 minutes before
/// giving up. Once connected, migration itself is not retried (a real DDL failure
/// should surface).
pub async fn run_migrations(db: &DbConfig) -> Result<(), RuntimeError> {
    let pool = connect_pool_waiting(db).await?;
    control_plane_postgres::run_embedded_migrations(&pool)
        .await
        .map_err(RuntimeError::Migrate)?;
    Ok(())
}

/// Build a pool, retrying the initial connection while the database is unreachable.
/// 60 attempts × 2s ≈ 2 minutes, covering a bundled Postgres still coming up.
async fn connect_pool_waiting(db: &DbConfig) -> Result<PgPool, RuntimeError> {
    let mut attempt: u32 = 0;
    loop {
        match build_pool(db).await {
            Ok(pool) => return Ok(pool),
            Err(e) if attempt < 60 => {
                attempt = attempt.saturating_add(1);
                tracing::warn!(
                    "waiting for database to accept connections (attempt {attempt}): {e}"
                );
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
            Err(e) => return Err(e),
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
    serve_with_shutdown(listener, router, std::future::pending()).await
}

/// Serve `router` on an already-bound `listener`, returning once `shutdown` resolves.
/// Binding before the caller spawns this lets the caller guarantee the socket is
/// accepting before it signals readiness.
pub async fn serve_with_shutdown(
    listener: tokio::net::TcpListener,
    router: Router,
    shutdown: impl std::future::Future<Output = ()> + Send + 'static,
) -> Result<(), RuntimeError> {
    axum::serve(listener, router)
        .with_graceful_shutdown(shutdown)
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
