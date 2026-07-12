//! The query-api service seam: connect the engine clients, build the read router,
//! and serve it on a pre-bound TCP listener until `shutdown` resolves.
use std::future::Future;
use std::sync::Arc;

use control_plane_core::ControlPlane;

use crate::engine_client::EngineServingClient;
use crate::http::{AppState, router};
use crate::serving::{ActionEngine, ServingEngine};

type BoxErr = Box<dyn std::error::Error + Send + Sync>;

pub async fn serve(
    cfg: &service_runtime::Config,
    direct: Arc<dyn ControlPlane>,
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

    // Clones for the optional external Flight SQL wire, captured before `cp`/`auth` move —
    // same pattern as the export's `cp_flight`/`auth_flight` above.
    let cp_sql = cp.clone();
    let auth_sql: Arc<dyn control_plane_core::Auth + Send + Sync> = auth.auth.clone();

    let admin_state = service_runtime::AdminState {
        auth: auth.auth.clone(),
        cp: direct.clone(),
    };
    let max_ttl = service_runtime::service_token_max_ttl(&env)?;

    let naming = std::sync::Arc::new(lineage_naming::LineageNaming::from_object_store(
        &cfg.object_store,
    ));

    let app = service_runtime::protect(
        router(AppState {
            cp,
            serving,
            action_engine,
            default_limit: app_cfg.serving.default_limit,
            gc_retention: cfg.gc_retention,
            naming,
        }),
        auth.clone(),
    )
    .merge(service_runtime::login_routes(auth.clone()))
    .merge(service_runtime::session_routes(auth.clone()))
    .merge(service_runtime::admin_routes(admin_state, auth.clone()))
    .merge(service_runtime::service_account_routes(
        auth,
        direct.clone(),
        max_ttl,
    ));

    // Dynamic OpenAPI: `/openapi.json` regenerates per request from the LIVE ontology, read
    // through the direct Postgres control plane rather than the engine-wire proxy — the
    // docs endpoint needs no engine dependency. `/docs` (Scalar) loads the live spec by URL.
    let openapi_cp: Arc<dyn ControlPlane> = direct;
    let app = service_runtime::with_openapi_provider(app, move || {
        let cp = openapi_cp.clone();
        async move { crate::live_openapi(cp).await }
    });

    // Optional UI serving (tight deploy) + CORS (detached deploy); both default off.
    let app =
        crate::web_static::with_static(app, env.get("LOOM_UI_DIR").map(std::path::PathBuf::from));
    let origins = crate::web_static::parse_allowed_origins(
        env.get("LOOM_CORS_ALLOWED_ORIGINS")
            .map_or("", String::as_str),
    );
    let app = crate::web_static::with_cors(app, &origins);

    // Optional external Arrow Flight export listener (opt-in via LOOM_FLIGHT_BIND_ADDR;
    // typed seam — its knobs are the typed `FlightExportTuning`, validated at config
    // load, not raw env reads, per road-flight-export-config-seam).
    if let Some(bind) = app_cfg.flight_export.bind_addr.clone() {
        spawn_flight_export(
            &bind,
            &engine_socket,
            auth_flight,
            cp_flight,
            app_cfg.flight_export.max_rows,
        )
        .await?;
    }

    // Optional external Flight SQL wire (opt-in via LOOM_SQL_WIRE_BIND_ADDR; typed seam —
    // its knobs are the typed `SqlWireTuning`, the twin of the flight-export block above).
    if let Some(bind) = app_cfg.sql_wire.bind_addr.clone() {
        spawn_sql_wire(
            &bind,
            &engine_socket,
            auth_sql,
            cp_sql,
            app_cfg.sql_wire.max_rows,
        )
        .await?;
    }

    service_runtime::serve_with_shutdown(listener, app, shutdown).await?;
    Ok(())
}

async fn spawn_flight_export(
    bind: &str,
    engine_socket: &str,
    auth_flight: Arc<dyn control_plane_core::Auth + Send + Sync>,
    cp_flight: Arc<dyn ControlPlane>,
    max_rows: u32,
) -> Result<(), BoxErr> {
    use arrow_flight::flight_service_server::FlightServiceServer;

    use crate::flight_export::FlightExportService;

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

async fn spawn_sql_wire(
    bind: &str,
    engine_socket: &str,
    auth_sql: Arc<dyn control_plane_core::Auth + Send + Sync>,
    cp_sql: Arc<dyn ControlPlane>,
    max_rows: u32,
) -> Result<(), BoxErr> {
    use arrow_flight::flight_service_server::FlightServiceServer;

    use crate::flight_sql::FlightSqlWireService;

    let addr: std::net::SocketAddr = bind.parse().map_err(|e| -> BoxErr {
        format!("LOOM_SQL_WIRE_BIND_ADDR `{bind}` invalid: {e}").into()
    })?;
    let flight_engine =
        engine_wire::flight::FlightSqlClient::connect(engine_socket.to_string()).await?;
    let wire = FlightSqlWireService::new(auth_sql, cp_sql, flight_engine, max_rows);

    // Bind eagerly so an operator who explicitly requested the SQL wire gets a hard
    // startup failure (port in use, permission) rather than a silently-down listener.
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .map_err(|e| -> BoxErr {
            format!("binding LOOM_SQL_WIRE_BIND_ADDR `{addr}` failed: {e}").into()
        })?;
    let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
    let _sql_wire_task = tokio::spawn(async move {
        tracing::info!(%addr, "starting external Flight SQL wire");
        if let Err(e) = tonic::transport::Server::builder()
            .add_service(FlightServiceServer::new(wire))
            .serve_with_incoming(incoming)
            .await
        {
            tracing::error!(error = %e, "Flight SQL wire server exited");
        }
    });
    Ok(())
}
