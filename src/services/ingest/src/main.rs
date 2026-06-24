//! ingest binary: build the landing AppState from the environment via service_runtime
//! and serve the HTTP API.

use std::collections::HashMap;
use std::sync::Arc;

use control_plane_postgres::iceberg_sql_catalog::{
    SQL_CATALOG_PROP_URI, SQL_CATALOG_PROP_WAREHOUSE, SqlCatalog, SqlCatalogBuilder,
};
use iceberg::CatalogBuilder;
use ingest::http::{AppState, router};
use ingest::landing::{
    DuckLakeMaterializer, IcebergMaterializer, LandingBackend, LandingMaterializer,
    parse_landing_backend,
};

/// Default inline threshold: 16 MiB of in-memory (uncompressed) Arrow. Below this a
/// request inlines (mirror-only rows); above it writes real Parquet. Tunable via
/// `LOOM_INLINE_BYTE_LIMIT`.
const DEFAULT_INLINE_BYTE_LIMIT: usize = 16 * 1024 * 1024;

/// Default live-inline-byte total that triggers a flush, overridable via
/// `LOOM_FLUSH_BYTE_THRESHOLD`. 64 MiB = 4× the inline routing limit, so a table
/// accrues several inline batches before compacting.
const DEFAULT_FLUSH_BYTE_THRESHOLD: i64 = 64 * 1024 * 1024;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    service_runtime::init_tracing();
    let cfg = service_runtime::Config::from_env()?;
    let pool = service_runtime::build_pool(&cfg.db).await?;

    let backend = parse_landing_backend(std::env::var("LOOM_LANDING_BACKEND").ok().as_deref())
        .map_err(|e| -> Box<dyn std::error::Error> { e.into() })?;

    let materializer: Arc<dyn LandingMaterializer> = match backend {
        LandingBackend::DuckLake => {
            let cp = Arc::new(service_runtime::control_plane(pool, cfg.lock_timeout));
            let store = Arc::new(service_runtime::local_store(&cfg.data_path)?);
            Arc::new(DuckLakeMaterializer { cp, store })
        }
        LandingBackend::Iceberg => {
            let inline_byte_limit = std::env::var("LOOM_INLINE_BYTE_LIMIT")
                .ok()
                .and_then(|v| v.parse::<usize>().ok())
                .unwrap_or(DEFAULT_INLINE_BYTE_LIMIT);
            let flush_byte_threshold = std::env::var("LOOM_FLUSH_BYTE_THRESHOLD")
                .ok()
                .and_then(|v| v.parse::<i64>().ok())
                .unwrap_or(DEFAULT_FLUSH_BYTE_THRESHOLD);
            let catalog = Arc::new(build_iceberg_catalog(&cfg).await?);
            Arc::new(IcebergMaterializer {
                catalog,
                pool,
                inline_byte_limit,
                flush_byte_threshold,
            })
        }
    };

    let app = router(AppState { materializer });
    service_runtime::serve(cfg.bind_addr, app).await?;
    Ok(())
}

/// Construct the vendored Iceberg SQL catalog over the same Postgres + a `file://`
/// warehouse rooted at the service data path.
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
