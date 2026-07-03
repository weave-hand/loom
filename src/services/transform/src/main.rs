//! transform binary: build the control plane + object store from env config via
//! service_runtime, then run the queue worker loop dispatching the two transform
//! handlers by job kind. Queue-driven — no HTTP surface.

use std::collections::HashMap;
use std::sync::Arc;

use control_plane_core::{ControlPlane, Job, JobFailure};
use control_plane_postgres::iceberg_control_plane::IcebergControlPlane;
use control_plane_postgres::iceberg_sql_catalog::{
    SQL_CATALOG_PROP_URI, SQL_CATALOG_PROP_WAREHOUSE, SqlCatalog, SqlCatalogBuilder,
};
use control_plane_worker::Worker;
use iceberg::CatalogBuilder;
use object_store::ObjectStore;
use tokio_util::sync::CancellationToken;
use transform::{transform_handler, typed_transform_handler};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cfg = service_runtime::Config::from_env()?;
    // Compose transform config as defaults < file < env (see `JobConfig`'s `LayeredConfig`).
    let env = service_runtime::env_map();
    let tcfg: datafusion_io::JobConfig = service_runtime::load(&env)?;
    let pool = service_runtime::build_pool(&cfg.db).await?;
    // The queue is backend-neutral (same Postgres tables either way), so the Worker
    // always dequeues through the `PgControlPlane`; the *handler's* `ControlPlane`
    // (which `run.rs` commits output through) writes to Iceberg.
    let pg = service_runtime::control_plane(pool, cfg.lock_timeout);
    // The write store carries the warehouse `root_url` (scheme-selected `file://`/`s3://`),
    // which the handlers absolutize the output's data-file paths against so the committed
    // mirror paths match what the serving engine resolves.
    let write = service_runtime::build_write_store(&cfg.object_store)?;
    let store: Arc<dyn ObjectStore> = write.store.clone();
    let root_url = write.root_url.clone();
    let write_cfg = tcfg.write;

    let catalog = build_iceberg_catalog(&cfg).await?;
    let cp_for_handler: Arc<dyn ControlPlane> =
        Arc::new(IcebergControlPlane::new(pg.clone(), catalog));
    let worker = Worker::new(pg, "transform-1", cfg.lock_timeout)
        .with_poll_interval(tcfg.worker.poll_interval());
    let shutdown = CancellationToken::new();

    worker
        .run(
            &["transform".to_string(), "typed-transform".to_string()],
            shutdown,
            move |job: Job| {
                let cp = cp_for_handler.clone();
                let store = store.clone();
                let root_url = root_url.clone();
                let write_cfg = write_cfg.clone();
                async move {
                    match job.kind.as_str() {
                        "typed-transform" => {
                            typed_transform_handler(cp.as_ref(), store, &root_url, &write_cfg, job)
                                .await
                        }
                        "transform" => {
                            transform_handler(cp.as_ref(), store, &root_url, &write_cfg, job).await
                        }
                        other => Err(JobFailure::abandon(format!("unknown job kind: {other}"))),
                    }
                }
            },
        )
        .await?;
    Ok(())
}

/// Construct the vendored Iceberg SQL catalog over the same Postgres + the configured
/// object-store warehouse (mirrors `ingest::build_iceberg_catalog`).
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
