//! query-api binary: build the read AppState from env config via service_runtime —
//! a Postgres control plane plus the loom-native DataFusion engine over the Iceberg
//! mirror (reads + governed writes stream over the engine wire) — and serve the HTTP API.

use std::sync::Arc;

use control_plane_core::ControlPlane;
use query_api::engine_client::EngineServingClient;
use query_api::http::{AppState, router};
use query_api::serving::{ActionEngine, ServingEngine};

/// Default per-export row cap (`LOOM_EXPORT_MAX_ROWS`). Bounds a runaway governed export; an
/// operator hydrating a large working set raises it.
const DEFAULT_EXPORT_MAX_ROWS: u32 = 1_000_000;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    service_runtime::init_tracing();
    let cfg = service_runtime::Config::from_env()?;
    let pool = service_runtime::build_pool(&cfg.db).await?;

    // Compose query-api config as defaults < file < env (see `QueryApiConfig`'s `LayeredConfig`).
    let env = service_runtime::env_map();
    let app_cfg: query_api::config::QueryApiConfig = service_runtime::load(&env)?;

    // Concrete PgControlPlane: retained for Auth, bootstrap_admin, and the GC enqueue
    // (via WireControlPlane::queue()). Governance reads (ACL + ontology) go over the wire.
    let pg = Arc::new(service_runtime::control_plane(
        pool.clone(),
        cfg.lock_timeout,
    ));

    let engine_socket =
        std::env::var("LOOM_ENGINE_SOCKET").map_err(|e| -> Box<dyn std::error::Error> {
            format!("LOOM_ENGINE_SOCKET must be set for the Iceberg serving backend: {e}").into()
        })?;

    // Governance reads relocate to the engine wire; queue() still delegates to `pg`.
    let gov_client = engine_wire::client::GrpcQueueClient::connect(engine_socket.clone()).await?;
    let cp: Arc<dyn ControlPlane> = Arc::new(query_api::wire_control_plane::WireControlPlane::new(
        gov_client,
        pg.clone() as Arc<dyn ControlPlane>,
    ));

    let (serving, action_engine): (Arc<dyn ServingEngine>, Arc<dyn ActionEngine>) = (
        Arc::new(EngineServingClient::connect(engine_socket.clone()).await?),
        Arc::new(
            query_api::engine_action_client::EngineActionClient::connect(engine_socket.clone())
                .await?,
        ),
    );

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

    // Clones for the optional Flight export server, captured before `cp` is moved into AppState.
    let cp_flight = cp.clone();
    let auth_flight: std::sync::Arc<dyn control_plane_core::Auth + Send + Sync> = pg.clone();

    let admin_subject = std::env::var("LOOM_BOOTSTRAP_ADMIN_USERNAME")
        .ok()
        .map(control_plane_core::SubjectId);
    let max_ttl = service_runtime::service_token_max_ttl_from_env();

    let app = service_runtime::protect(
        router(AppState {
            cp,
            serving,
            action_engine,
            default_limit: app_cfg.serving.default_limit,
        }),
        auth_state.clone(),
    )
    .merge(service_runtime::login_routes(auth_state.clone()))
    .merge(service_runtime::session_routes(auth_state.clone()))
    .merge(service_runtime::service_account_routes(
        auth_state,
        admin_subject,
        max_ttl,
    ));
    let app = service_runtime::with_openapi(app, query_api::build_openapi());

    // Optional external Arrow Flight export listener (opt-in via LOOM_FLIGHT_BIND_ADDR).
    if let Ok(bind) = std::env::var("LOOM_FLIGHT_BIND_ADDR") {
        use arrow_flight::flight_service_server::FlightServiceServer;
        use query_api::flight_export::FlightExportService;

        let max_rows = std::env::var("LOOM_EXPORT_MAX_ROWS")
            .ok()
            .and_then(|v| v.parse::<u32>().ok())
            .unwrap_or(DEFAULT_EXPORT_MAX_ROWS);
        let addr: std::net::SocketAddr =
            bind.parse().map_err(|e| -> Box<dyn std::error::Error> {
                format!("LOOM_FLIGHT_BIND_ADDR `{bind}` is not a valid socket address: {e}").into()
            })?;
        let flight_engine =
            engine_wire::flight::FlightSqlClient::connect(engine_socket.clone()).await?;
        let export = FlightExportService::new(auth_flight, cp_flight, flight_engine, max_rows);

        // Bind eagerly so an operator who explicitly requested the export endpoint gets a hard
        // startup failure (port in use, permission) rather than a silently-down listener.
        let listener = tokio::net::TcpListener::bind(addr).await.map_err(
            |e| -> Box<dyn std::error::Error> {
                format!("binding LOOM_FLIGHT_BIND_ADDR `{addr}` failed: {e}").into()
            },
        )?;
        let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
        tokio::spawn(async move {
            tracing::info!(%addr, "starting governed Flight export server");
            if let Err(e) = tonic::transport::Server::builder()
                .add_service(FlightServiceServer::new(export))
                .serve_with_incoming(incoming)
                .await
            {
                tracing::error!(error = %e, "Flight export server exited");
            }
        });
    }

    service_runtime::serve(cfg.bind_addr, app).await?;
    Ok(())
}
