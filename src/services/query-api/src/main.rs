//! query-api binary: build the read AppState from env config via service_runtime —
//! a Postgres control plane plus the loom-native DataFusion engine over the Iceberg
//! mirror (reads stream over the engine wire via `EngineServingClient`) — and serve
//! the HTTP API.

use std::collections::HashMap;
use std::sync::Arc;

use control_plane_core::ControlPlane;
use control_plane_postgres::iceberg_sql_catalog::{
    SQL_CATALOG_PROP_URI, SQL_CATALOG_PROP_WAREHOUSE, SqlCatalog, SqlCatalogBuilder,
};
use iceberg::CatalogBuilder;
use query_api::engine_client::EngineServingClient;
use query_api::http::{AppState, router};
use query_api::serving::{ActionEngine, ServingEngine};
use query_api::serving_datafusion::IcebergActionWriter;

/// Inline routing threshold (in-memory uncompressed Arrow). Below this an action row
/// inlines (mirror-only); tunable via `LOOM_INLINE_BYTE_LIMIT`. Matches ingest.
const DEFAULT_INLINE_BYTE_LIMIT: usize = 16 * 1024 * 1024;
/// Live-inline-byte total that triggers a flush, via `LOOM_FLUSH_BYTE_THRESHOLD`.
const DEFAULT_FLUSH_BYTE_THRESHOLD: i64 = 64 * 1024 * 1024;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    service_runtime::init_tracing();
    let cfg = service_runtime::Config::from_env()?;
    let pool = service_runtime::build_pool(&cfg.db).await?;

    // Concrete PgControlPlane: serves both ControlPlane (read path) and Auth.
    let pg = Arc::new(service_runtime::control_plane(
        pool.clone(),
        cfg.lock_timeout,
    ));
    let cp: Arc<dyn ControlPlane> = pg.clone();

    let (serving, action_engine): (Arc<dyn ServingEngine>, Arc<dyn ActionEngine>) = {
        let inline_byte_limit = std::env::var("LOOM_INLINE_BYTE_LIMIT")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(DEFAULT_INLINE_BYTE_LIMIT);
        let flush_byte_threshold = std::env::var("LOOM_FLUSH_BYTE_THRESHOLD")
            .ok()
            .and_then(|v| v.parse::<i64>().ok())
            .unwrap_or(DEFAULT_FLUSH_BYTE_THRESHOLD);
        let engine_socket =
            std::env::var("LOOM_ENGINE_SOCKET").map_err(|e| -> Box<dyn std::error::Error> {
                format!("LOOM_ENGINE_SOCKET must be set for the Iceberg serving backend: {e}")
                    .into()
            })?;
        let catalog = Arc::new(build_iceberg_catalog(&cfg).await?);
        let action: Arc<dyn ActionEngine> = Arc::new(IcebergActionWriter::new(
            catalog,
            pool.clone(),
            inline_byte_limit,
            flush_byte_threshold,
        ));
        (
            Arc::new(EngineServingClient::connect(engine_socket).await?),
            action,
        )
    };

    // Auth wiring.
    let auth_state = service_runtime::AuthState {
        auth: pg.clone(),
        session_ttl: service_runtime::session_ttl_from_env(),
    };
    if let (Ok(user), Ok(pass)) = (
        std::env::var("LOOM_BOOTSTRAP_ADMIN_USERNAME"),
        std::env::var("LOOM_BOOTSTRAP_ADMIN_PASSWORD"),
    ) {
        service_runtime::bootstrap_admin(pg.as_ref(), &user, &pass).await?;
    }

    let app = service_runtime::protect(
        router(AppState {
            cp,
            serving,
            action_engine,
        }),
        auth_state.clone(),
    )
    .merge(service_runtime::login_routes(auth_state.clone()))
    .merge(service_runtime::session_routes(auth_state));

    service_runtime::serve(cfg.bind_addr, app).await?;
    Ok(())
}

/// Construct the vendored Iceberg SQL catalog over the same Postgres using the
/// configured warehouse URI (scheme-selected: `file://` for local, `s3://` for S3).
/// Mirrors ingest's helper.
async fn build_iceberg_catalog(
    cfg: &service_runtime::Config,
) -> Result<SqlCatalog, Box<dyn std::error::Error>> {
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
    Ok(catalog)
}
