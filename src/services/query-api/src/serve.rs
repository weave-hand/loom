//! The query-api service seam: connect the engine clients, build the read router,
//! and serve it on a pre-bound TCP listener until `shutdown` resolves.
use std::future::Future;
use std::sync::Arc;

use control_plane_core::ControlPlane;

use crate::engine_client::EngineServingClient;
use crate::http::{AppState, router};
use crate::serving::{ActionEngine, ServingEngine};

type BoxErr = Box<dyn std::error::Error + Send + Sync>;

/// Default per-export row cap (`LOOM_EXPORT_MAX_ROWS`). Bounds a runaway governed export; an
/// operator hydrating a large working set raises it.
const DEFAULT_EXPORT_MAX_ROWS: u32 = 1_000_000;

pub async fn serve(
    _cfg: &service_runtime::Config,
    direct: Arc<dyn ControlPlane>,
    _acl: Arc<dyn control_plane_core::Acl + Send + Sync>,
    auth: service_runtime::AuthState,
    engine_socket: String,
    listener: tokio::net::TcpListener,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> Result<(), BoxErr> {
    let env = service_runtime::env_map();
    let app_cfg: crate::config::QueryApiConfig = service_runtime::load(&env)?;

    // Governance reads relocate to the engine wire; queue() still delegates to `pg`.
    let gov_client = engine_wire::client::GrpcQueueClient::connect(engine_socket.clone()).await?;
    let cp: Arc<dyn ControlPlane> = Arc::new(crate::wire_control_plane::WireControlPlane::new(
        gov_client,
        direct.clone(),
    ));

    let (serving, action_engine): (Arc<dyn ServingEngine>, Arc<dyn ActionEngine>) = (
        Arc::new(EngineServingClient::connect(engine_socket.clone()).await?),
        Arc::new(
            crate::engine_action_client::EngineActionClient::connect(engine_socket.clone()).await?,
        ),
    );

    // Clones for the optional Flight export server, captured before `cp` is moved into AppState.
    let cp_flight = cp.clone();
    let auth_flight: Arc<dyn control_plane_core::Auth + Send + Sync> = auth.auth.clone();

    let admin_state = service_runtime::AdminState {
        auth: auth.auth.clone(),
        cp: direct.clone(),
    };
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
        auth.clone(),
    )
    .merge(service_runtime::login_routes(auth.clone()))
    .merge(service_runtime::session_routes(auth.clone()))
    .merge(service_runtime::admin_routes(admin_state, auth.clone()))
    .merge(service_runtime::service_account_routes(
        auth,
        admin_subject,
        max_ttl,
    ));

    // Dynamic OpenAPI: `/openapi.json` regenerates per request from the LIVE ontology, read
    // through the direct Postgres control plane (`pg`) — the wire governance client does not
    // implement `list_types`. `/docs` (Scalar) loads the live spec by URL.
    let openapi_cp: Arc<dyn ControlPlane> = direct;
    let app = service_runtime::with_openapi_provider(app, move || {
        let cp = openapi_cp.clone();
        async move { crate::live_openapi(cp).await }
    });

    // Optional UI serving (tight deploy) + CORS (detached deploy); both default off.
    let app = crate::web_static::with_static(
        app,
        std::env::var("LOOM_UI_DIR")
            .ok()
            .map(std::path::PathBuf::from),
    );
    let origins = crate::web_static::parse_allowed_origins(
        &std::env::var("LOOM_CORS_ALLOWED_ORIGINS").unwrap_or_default(),
    );
    let app = crate::web_static::with_cors(app, &origins);

    // Optional external Arrow Flight export listener (opt-in via LOOM_FLIGHT_BIND_ADDR).
    if let Ok(bind) = std::env::var("LOOM_FLIGHT_BIND_ADDR") {
        spawn_flight_export(&bind, &engine_socket, auth_flight, cp_flight).await?;
    }

    service_runtime::serve_with_shutdown(listener, app, shutdown).await?;
    Ok(())
}

async fn spawn_flight_export(
    bind: &str,
    engine_socket: &str,
    auth_flight: Arc<dyn control_plane_core::Auth + Send + Sync>,
    cp_flight: Arc<dyn ControlPlane>,
) -> Result<(), BoxErr> {
    use arrow_flight::flight_service_server::FlightServiceServer;

    use crate::flight_export::FlightExportService;

    let max_rows = std::env::var("LOOM_EXPORT_MAX_ROWS")
        .ok()
        .and_then(|v| v.parse::<u32>().ok())
        .unwrap_or(DEFAULT_EXPORT_MAX_ROWS);
    let addr: std::net::SocketAddr = bind
        .parse()
        .map_err(|e| -> BoxErr { format!("LOOM_FLIGHT_BIND_ADDR `{bind}` invalid: {e}").into() })?;
    let flight_engine =
        engine_wire::flight::FlightSqlClient::connect(engine_socket.to_string()).await?;
    let export = FlightExportService::new(auth_flight, cp_flight, flight_engine, max_rows);

    // Bind eagerly so an operator who explicitly requested the export endpoint gets a hard
    // startup failure (port in use, permission) rather than a silently-down listener.
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .map_err(|e| -> BoxErr {
            format!("binding LOOM_FLIGHT_BIND_ADDR `{addr}` failed: {e}").into()
        })?;
    let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
    let _flight_task = tokio::spawn(async move {
        tracing::info!(%addr, "starting governed Flight export server");
        if let Err(e) = tonic::transport::Server::builder()
            .add_service(FlightServiceServer::new(export))
            .serve_with_incoming(incoming)
            .await
        {
            tracing::error!(error = %e, "Flight export server exited");
        }
    });
    Ok(())
}
