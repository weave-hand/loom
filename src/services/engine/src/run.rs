//! The engine service seam: build the control + flight services over a shared
//! pool and serve them on a pre-bound Unix socket until `shutdown` resolves.
use std::collections::HashMap;
use std::future::Future;

use arrow_flight::flight_service_server::FlightServiceServer;
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use control_plane_postgres::iceberg_sql_catalog::{
    SQL_CATALOG_PROP_URI, SQL_CATALOG_PROP_WAREHOUSE, SqlCatalogBuilder,
};
use iceberg::CatalogBuilder;
use tokio::net::UnixListener;
use tokio::sync::oneshot;
use tokio_stream::wrappers::UnixListenerStream;
use tonic::transport::Server;

use engine_wire::pb::engine_control_server::EngineControlServer;

use crate::flight::FlightDataService;
use crate::service::EngineControlService;

type BoxErr = Box<dyn std::error::Error + Send + Sync>;

/// Engine write-path byte thresholds, parsed once from the caller's env snapshot
/// (the engine main / the standalone composite) — `run` itself never reads the
/// live environment (one-env-snapshot-per-main).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EngineTuning {
    /// Row payloads at/below this size commit as inline PG rows
    /// (`LOOM_INLINE_BYTE_LIMIT`, default 16 MiB).
    pub inline_byte_limit: usize,
    /// Inline-row bytes above which a flush-to-Parquet job is enqueued
    /// (`LOOM_FLUSH_BYTE_THRESHOLD`, default 64 MiB).
    pub flush_byte_threshold: i64,
}

impl EngineTuning {
    /// Parse from the env snapshot: absent keys take the defaults, a
    /// present-but-malformed value fails startup naming the key.
    pub fn from_map(vars: &HashMap<String, String>) -> Result<Self, service_runtime::ConfigError> {
        Ok(EngineTuning {
            inline_byte_limit: service_runtime::parse_var(
                vars,
                "LOOM_INLINE_BYTE_LIMIT",
                16 * 1024 * 1024,
            )?,
            flush_byte_threshold: service_runtime::parse_var(
                vars,
                "LOOM_FLUSH_BYTE_THRESHOLD",
                64 * 1024 * 1024,
            )?,
        })
    }
}

/// Build and serve the engine on `listener`. Fires `ready` once the services are
/// built and the serve loop is about to run; returns when `shutdown` resolves.
pub async fn run(
    listener: UnixListener,
    cfg: &service_runtime::Config,
    pool: sqlx::PgPool,
    tuning: EngineTuning,
    ready: oneshot::Sender<()>,
    shutdown: impl Future<Output = ()> + Send,
) -> Result<(), BoxErr> {
    let cp = service_runtime::control_plane(pool.clone(), cfg.lock_timeout);

    let mut props = HashMap::new();
    props.insert(SQL_CATALOG_PROP_URI.to_string(), cfg.db.pg_url());
    props.insert(
        SQL_CATALOG_PROP_WAREHOUSE.to_string(),
        cfg.object_store.warehouse_uri.clone(),
    );
    let catalog = std::sync::Arc::new(
        SqlCatalogBuilder::default()
            .with_storage_factory(service_runtime::build_storage_factory(&cfg.object_store)?)
            .load("loom", props)
            .await?,
    );
    let writer = engine_serving::IcebergActionWriter::new(
        catalog.clone(),
        pool.clone(),
        tuning.inline_byte_limit,
        tuning.flush_byte_threshold,
    );

    let control = EngineControlService {
        cp,
        catalog: catalog.clone(),
        pool: pool.clone(),
        retention: cfg.gc_retention,
        writer,
    };
    let flight = FlightDataService {
        catalog,
        serving_catalog: IcebergCatalog::new(pool.clone()),
        serving_store: service_runtime::build_serving_object_store(&cfg.object_store)?,
        pool,
    };

    // Signal readiness: the caller binds `listener` before spawning us, so the
    // socket already accepts; this tells the caller the serve loop is starting.
    // The send fails only if the receiver dropped (e.g. throwaway in main.rs), which is fine.
    let _sent = ready.send(());

    Server::builder()
        .add_service(EngineControlServer::new(control))
        .add_service(FlightServiceServer::new(flight))
        .serve_with_incoming_shutdown(UnixListenerStream::new(listener), shutdown)
        .await?;
    Ok(())
}
