//! ingest binary: build the landing AppState from the environment via service_runtime
//! and serve the HTTP API.

use std::collections::HashMap;
use std::sync::Arc;

use control_plane_core::ControlPlane;
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

    // One concrete control plane, built before the `match backend` (the DuckLake
    // branch moves `pool` into its own materializer cp and the Iceberg branch moves
    // it into the catalog, so we clone here while `pool` is still owned). It serves
    // both the `ControlPlane` surface (the compact endpoint's queue) and `Auth`.
    let pg = Arc::new(service_runtime::control_plane(
        pool.clone(),
        cfg.lock_timeout,
    ));
    let cp: Arc<dyn ControlPlane> = pg.clone();

    let backend = parse_landing_backend(std::env::var("LOOM_LANDING_BACKEND").ok().as_deref())
        .map_err(|e| -> Box<dyn std::error::Error> { e.into() })?;

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

    let materializer: Arc<dyn LandingMaterializer> = match backend {
        LandingBackend::DuckLake => {
            let mat_cp = Arc::new(service_runtime::control_plane(pool, cfg.lock_timeout));
            let store = Arc::new(service_runtime::local_store(&cfg.data_path)?);
            Arc::new(DuckLakeMaterializer { cp: mat_cp, store })
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

    let app = service_runtime::protect(router(AppState { materializer, cp }), auth_state.clone())
        .merge(service_runtime::login_routes(auth_state.clone()))
        .merge(service_runtime::session_routes(auth_state));
    service_runtime::serve(cfg.bind_addr, app).await?;
    Ok(())
}

/// Construct the vendored Iceberg SQL catalog over the same Postgres using the
/// configured warehouse URI (scheme-selected: `file://` for local, `s3://` for S3).
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
