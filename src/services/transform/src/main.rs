//! transform binary: build the control plane + object store from env config via
//! service_runtime, then run the queue worker loop with the transform handler.
//! Queue-driven — no HTTP surface.

use std::sync::Arc;

use control_plane_core::{ControlPlane, Job};
use control_plane_worker::Worker;
use object_store::ObjectStore;
use tokio_util::sync::CancellationToken;
use transform::transform_handler;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cfg = service_runtime::Config::from_env()?;
    let pool = service_runtime::build_pool(&cfg.db).await?;
    let cp = service_runtime::control_plane(pool, cfg.lock_timeout);
    let store: Arc<dyn ObjectStore> = Arc::new(service_runtime::local_store(&cfg.data_path)?);

    let cp_for_handler: Arc<dyn ControlPlane> = Arc::new(cp.clone());
    let worker = Worker::new(cp, "transform-1", cfg.lock_timeout);
    let shutdown = CancellationToken::new();

    worker
        .run(&["transform".to_string()], shutdown, move |job: Job| {
            let cp = cp_for_handler.clone();
            let store = store.clone();
            async move { transform_handler(cp.as_ref(), store, job).await }
        })
        .await?;
    Ok(())
}
