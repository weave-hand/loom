//! query-api binary: shared setup, bind the HTTP listener, serve via `query_api::serve`.
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    service_runtime::init_tracing();
    let cfg = service_runtime::Config::from_env()?;
    // Migrate-and-exit mode: apply the control-plane schema and exit (chart hook Job).
    if service_runtime::migrate_requested() {
        service_runtime::run_migrations(&cfg.db).await?;
        return Ok(());
    }
    // query-api reads/writes over the engine wire; it needs the concrete control
    // plane but not the pool directly, so `pool` is consumed building `pg`.
    // `.1` is the embedded-PG handle (None in external mode); query-api is always
    // external, so it is discarded here but must stay alive for the embedded case.
    let (pool, _pg) = service_runtime::build_pool_managed(&cfg).await?;

    // Concrete PgControlPlane: retained for Auth and the GC enqueue (via
    // WireControlPlane::queue()). Governance reads (ACL + ontology) go over the wire.
    let pg = Arc::new(service_runtime::control_plane(pool, cfg.lock_timeout));
    let auth = service_runtime::AuthState {
        auth: pg.clone(),
        session_ttl: service_runtime::session_ttl_from_env(),
    };

    let engine_socket = std::env::var("LOOM_ENGINE_SOCKET").map_err(
        |e| -> Box<dyn std::error::Error + Send + Sync> {
            format!("LOOM_ENGINE_SOCKET must be set for the Iceberg serving backend: {e}").into()
        },
    )?;

    let listener = tokio::net::TcpListener::bind(cfg.bind_addr).await?;
    query_api::serve(
        &cfg,
        pg,
        auth,
        engine_socket,
        listener,
        std::future::pending(),
    )
    .await
}
