//! The ingest service seam: build the landing router over a shared pool and
//! serve it on a pre-bound TCP listener until `shutdown` resolves.
use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use control_plane_core::{ControlPlane, SubjectId};
use control_plane_postgres::iceberg_sql_catalog::{
    SQL_CATALOG_PROP_URI, SQL_CATALOG_PROP_WAREHOUSE, SqlCatalog, SqlCatalogBuilder,
};
use iceberg::CatalogBuilder;

use crate::http::{AppState, router};
use crate::landing::{IcebergMaterializer, LandingMaterializer};

type BoxErr = Box<dyn std::error::Error + Send + Sync>;

#[expect(
    clippy::too_many_arguments,
    reason = "seam function: all args are required singletons passed through from the binary"
)]
pub async fn serve(
    cfg: &service_runtime::Config,
    pool: sqlx::PgPool,
    cp: Arc<dyn ControlPlane>,
    auth: service_runtime::AuthState,
    admin_subject: Option<SubjectId>,
    max_ttl: Duration,
    listener: tokio::net::TcpListener,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> Result<(), BoxErr> {
    let env = service_runtime::env_map();
    let app_cfg: crate::config::IngestConfig = service_runtime::load(&env)?;

    let materializer: Arc<dyn LandingMaterializer> = {
        let catalog = Arc::new(build_iceberg_catalog(cfg).await?);
        Arc::new(IcebergMaterializer {
            catalog,
            pool,
            inline_byte_limit: app_cfg.routing.inline_byte_limit,
            flush_byte_threshold: app_cfg.routing.flush_byte_threshold,
        })
    };

    let app = service_runtime::protect(router(AppState { materializer, cp }), auth.clone())
        .merge(service_runtime::login_routes(auth.clone()))
        .merge(service_runtime::session_routes(auth.clone()))
        .merge(service_runtime::service_account_routes(
            auth,
            admin_subject,
            max_ttl,
        ));
    let app = service_runtime::with_openapi(app, crate::build_openapi());
    service_runtime::serve_with_shutdown(listener, app, shutdown).await?;
    Ok(())
}

async fn build_iceberg_catalog(cfg: &service_runtime::Config) -> Result<SqlCatalog, BoxErr> {
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
