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
use ingest::landing::{IcebergMaterializer, LandingMaterializer};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    service_runtime::init_tracing();
    let cfg = service_runtime::Config::from_env()?;
    let pool = service_runtime::build_pool(&cfg.db).await?;

    // One concrete control plane, built before the materializer (which moves `pool`
    // into the Iceberg catalog, so we clone here while `pool` is still owned). It
    // serves both the `ControlPlane` surface (the compact endpoint's queue) and `Auth`.
    let pg = Arc::new(service_runtime::control_plane(
        pool.clone(),
        cfg.lock_timeout,
    ));
    let cp: Arc<dyn ControlPlane> = pg.clone();

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

    // Compose ingest config as defaults < file < env (see `IngestConfig`'s `LayeredConfig`).
    // `app_cfg.write` is composed and validated here so LOOM_WRITE_* env vars parse and
    // validate at startup. The live HTTP landing path (IcebergMaterializer) does not consume
    // WriteConfig — only the datafusion write path (materialize::land) does. Do not add a
    // WriteConfig field to IcebergMaterializer this slice.
    let env = service_runtime::env_map();
    let app_cfg: ingest::config::IngestConfig = service_runtime::load(&env)?;

    let materializer: Arc<dyn LandingMaterializer> = {
        let catalog = Arc::new(build_iceberg_catalog(&cfg).await?);
        Arc::new(IcebergMaterializer {
            catalog,
            pool,
            inline_byte_limit: app_cfg.routing.inline_byte_limit,
            flush_byte_threshold: app_cfg.routing.flush_byte_threshold,
        })
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
