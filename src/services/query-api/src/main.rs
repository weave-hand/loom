//! query-api binary: build the read AppState from env config via service_runtime —
//! a Postgres control plane plus a serving engine selected by LOOM_SERVING_BACKEND
//! (DuckLake-on-DuckDB by default, or the loom-native DataFusion engine over the
//! Iceberg mirror) — and serve the HTTP API.

use std::collections::HashMap;
use std::sync::Arc;

use control_plane_core::ControlPlane;
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use control_plane_postgres::iceberg_sql_catalog::{
    SQL_CATALOG_PROP_URI, SQL_CATALOG_PROP_WAREHOUSE, SqlCatalog, SqlCatalogBuilder,
};
use iceberg::CatalogBuilder;
use iceberg::io::LocalFsStorageFactory;
use query_api::http::{AppState, router};
use query_api::serving::{ActionEngine, DuckLakeActionWriter, EmbeddedDuckDb, ServingEngine};
use query_api::serving_datafusion::{
    DataFusionServingEngine, IcebergActionWriter, ServingBackend, parse_serving_backend,
};

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
    let backend = parse_serving_backend(std::env::var("LOOM_SERVING_BACKEND").ok().as_deref())
        .map_err(|e| -> Box<dyn std::error::Error> { e.into() })?;

    // cp (ontology + ACL) is format-agnostic and identical for both backends.
    let cp: Arc<dyn ControlPlane> = Arc::new(service_runtime::control_plane(
        pool.clone(),
        cfg.lock_timeout,
    ));

    let (serving, action_engine): (Arc<dyn ServingEngine>, Arc<dyn ActionEngine>) = match backend {
        ServingBackend::DuckLake => {
            let store: Arc<dyn object_store::ObjectStore> =
                Arc::new(service_runtime::local_store(&cfg.data_path)?);
            (
                Arc::new(EmbeddedDuckDb::attach(&cfg.db.ducklake_libpq(), &cfg.data_path).await?),
                Arc::new(DuckLakeActionWriter::new(cp.clone(), store)),
            )
        }
        ServingBackend::Iceberg => {
            let inline_byte_limit = std::env::var("LOOM_INLINE_BYTE_LIMIT")
                .ok()
                .and_then(|v| v.parse::<usize>().ok())
                .unwrap_or(DEFAULT_INLINE_BYTE_LIMIT);
            let flush_byte_threshold = std::env::var("LOOM_FLUSH_BYTE_THRESHOLD")
                .ok()
                .and_then(|v| v.parse::<i64>().ok())
                .unwrap_or(DEFAULT_FLUSH_BYTE_THRESHOLD);
            let catalog = Arc::new(build_iceberg_catalog(&cfg).await?);
            // Clone the pool for the action writer before the bare `pool` moves into
            // the serving engine's `IcebergCatalog`.
            let action: Arc<dyn ActionEngine> = Arc::new(IcebergActionWriter::new(
                catalog,
                pool.clone(),
                inline_byte_limit,
                flush_byte_threshold,
            ));
            (
                Arc::new(DataFusionServingEngine::new(IcebergCatalog::new(pool))),
                action,
            )
        }
    };

    let app = router(AppState {
        cp,
        serving,
        action_engine,
    });
    service_runtime::serve(cfg.bind_addr, app).await?;
    Ok(())
}

/// Construct the vendored Iceberg SQL catalog over the same Postgres + a `file://`
/// warehouse rooted at the service data path. Mirrors ingest's helper.
async fn build_iceberg_catalog(
    cfg: &service_runtime::Config,
) -> Result<SqlCatalog, Box<dyn std::error::Error>> {
    let mut props = HashMap::new();
    props.insert(SQL_CATALOG_PROP_URI.to_string(), cfg.db.pg_url());
    props.insert(
        SQL_CATALOG_PROP_WAREHOUSE.to_string(),
        format!("file://{}", cfg.data_path.display()),
    );
    let catalog = SqlCatalogBuilder::default()
        .with_storage_factory(Arc::new(LocalFsStorageFactory))
        .load("loom", props)
        .await?;
    Ok(catalog)
}
