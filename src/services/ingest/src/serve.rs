//! The ingest service seam: build the landing router over a shared pool and
//! serve it on a pre-bound TCP listener until `shutdown` resolves.
use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use control_plane_core::ControlPlane;
use control_plane_postgres::iceberg_compact::CompactTriggerCfg;
use control_plane_postgres::iceberg_sql_catalog::{
    SQL_CATALOG_PROP_URI, SQL_CATALOG_PROP_WAREHOUSE, SqlCatalog, SqlCatalogBuilder,
};
use iceberg::CatalogBuilder;

use crate::config::RoutingTuning;
use crate::http::{AppState, router};
use crate::landing::{IcebergMaterializer, LandingMaterializer};

type BoxErr = Box<dyn std::error::Error + Send + Sync>;

pub async fn serve(
    cfg: &service_runtime::Config,
    pool: sqlx::PgPool,
    cp: Arc<dyn ControlPlane>,
    auth: service_runtime::AuthState,
    max_ttl: Duration,
    listener: tokio::net::TcpListener,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> Result<(), BoxErr> {
    let env = service_runtime::env_map();
    let app_cfg: crate::config::IngestConfig = service_runtime::load(&env)?;

    let compact_small_file_bytes = app_cfg.routing.compact_small_file_bytes;
    // The materializer takes ownership of the pool; the operator compact surface
    // needs its own handle (`PgPool` is an Arc'd handle — cloning is cheap).
    let state_pool = pool.clone();

    let materializer: Arc<dyn LandingMaterializer> = {
        let catalog = Arc::new(build_iceberg_catalog(cfg, &app_cfg.routing).await?);
        Arc::new(IcebergMaterializer {
            catalog,
            pool,
            inline_byte_limit: app_cfg.routing.inline_byte_limit,
            flush_byte_threshold: app_cfg.routing.flush_byte_threshold,
        })
    };

    let sa_cp = cp.clone();
    let state = AppState {
        materializer,
        cp,
        pool: state_pool,
        compact_small_file_bytes,
    };
    let app = service_runtime::protect(router(state), auth.clone())
        .merge(service_runtime::login_routes(auth.clone()))
        .merge(service_runtime::session_routes(auth.clone()))
        .merge(service_runtime::service_account_routes(
            auth, sa_cp, max_ttl,
        ));
    // Raise axum's stock 2 MB body cap: Arrow IPC bodies must be able to
    // exceed inline_byte_limit or the Parquet branch is unreachable (#370).
    let app = app.layer(axum::extract::DefaultBodyLimit::max(
        app_cfg.routing.http_max_body_bytes,
    ));
    let app = service_runtime::with_openapi(app, crate::build_openapi());
    service_runtime::serve_with_shutdown(listener, app, shutdown).await?;
    Ok(())
}

async fn build_iceberg_catalog(
    cfg: &service_runtime::Config,
    routing: &RoutingTuning,
) -> Result<SqlCatalog, BoxErr> {
    let mut props = HashMap::new();
    props.insert(SQL_CATALOG_PROP_URI.to_string(), cfg.db.pg_url());
    props.insert(
        SQL_CATALOG_PROP_WAREHOUSE.to_string(),
        cfg.object_store.warehouse_uri.clone(),
    );
    let catalog = SqlCatalogBuilder::default()
        .with_storage_factory(service_runtime::build_storage_factory(&cfg.object_store)?)
        .load("loom", props)
        .await?;
    let catalog = if routing.compact_trigger_files >= 2 {
        catalog.with_compact_trigger(CompactTriggerCfg {
            small_file_bytes: routing.compact_small_file_bytes,
            min_small_files: routing.compact_trigger_files,
        })
    } else {
        catalog
    };
    Ok(catalog)
}
