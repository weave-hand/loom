//! The standalone composite: boot the embedded Postgres once and run engine
//! (tonic/UDS) + ingest (HTTP) + query-api (HTTP) as tasks in one runtime.
use std::future::Future;
use std::sync::Arc;

use control_plane_core::ControlPlane;

type BoxErr = Box<dyn std::error::Error + Send + Sync>;

/// The three listen addresses the composite binds.
pub struct StandaloneAddrs {
    pub query_api: std::net::SocketAddr,
    pub ingest: std::net::SocketAddr,
    pub engine_socket: String,
}

/// Boot the composite. Binds all listeners before spawning their serve loops so
/// readiness is race-free; `ready` fires once every listener is bound. On
/// `shutdown` the three servers stop, then the embedded PG is stopped last.
pub async fn run(
    cfg: service_runtime::Config,
    addrs: StandaloneAddrs,
    shutdown: impl Future<Output = ()> + Send + 'static,
    ready: tokio::sync::oneshot::Sender<()>,
) -> Result<(), BoxErr> {
    // Shared singletons, built once.
    let (pool, pg_handle) = service_runtime::build_pool_managed(&cfg).await?;
    let pg = Arc::new(service_runtime::control_plane(
        pool.clone(),
        cfg.lock_timeout,
    ));
    let auth = service_runtime::AuthState {
        auth: pg.clone(),
        session_ttl: service_runtime::session_ttl_from_env(),
    };
    if let (Ok(user), Ok(pass)) = (
        std::env::var("LOOM_BOOTSTRAP_ADMIN_USERNAME"),
        std::env::var("LOOM_BOOTSTRAP_ADMIN_PASSWORD"),
    ) {
        service_runtime::bootstrap_admin(pg.as_ref(), &user, &pass).await?;
    }
    let admin_subject = std::env::var("LOOM_BOOTSTRAP_ADMIN_USERNAME")
        .ok()
        .map(control_plane_core::SubjectId);
    let max_ttl = service_runtime::service_token_max_ttl_from_env();

    // One shutdown source fanned out to all three servers via a watch channel.
    let (sd_tx, sd_rx) = tokio::sync::watch::channel(false);
    tokio::spawn(async move {
        shutdown.await;
        let _sent = sd_tx.send(true);
    });
    let sub = |rx: tokio::sync::watch::Receiver<bool>| async move {
        let mut rx = rx;
        while !*rx.borrow() {
            if rx.changed().await.is_err() {
                break;
            }
        }
    };

    // Engine first: bind UDS synchronously, spawn, await engine-ready.
    drop(std::fs::remove_file(&addrs.engine_socket));
    let engine_listener = tokio::net::UnixListener::bind(&addrs.engine_socket)?;
    let (eng_ready_tx, eng_ready_rx) = tokio::sync::oneshot::channel();
    let engine_cfg = cfg.clone();
    let engine_pool = pool.clone();
    let engine_sd = sub(sd_rx.clone());
    let engine = tokio::spawn(async move {
        engine::run(
            engine_listener,
            &engine_cfg,
            engine_pool,
            eng_ready_tx,
            engine_sd,
        )
        .await
    });
    eng_ready_rx
        .await
        .map_err(|_e| -> BoxErr { "engine exited before signalling ready".into() })?;

    // Bind both HTTP listeners, then spawn their serves.
    let ingest_listener = tokio::net::TcpListener::bind(addrs.ingest).await?;
    let qapi_listener = tokio::net::TcpListener::bind(addrs.query_api).await?;

    let ingest_cfg = cfg.clone();
    let ingest_cp: Arc<dyn ControlPlane> = pg.clone();
    let ingest_auth = auth.clone();
    let ingest_sd = sub(sd_rx.clone());
    let ingest = tokio::spawn(async move {
        ingest::serve(
            &ingest_cfg,
            pool.clone(),
            ingest_cp,
            ingest_auth,
            admin_subject.clone(),
            max_ttl,
            ingest_listener,
            ingest_sd,
        )
        .await
    });

    let qapi_cfg = cfg.clone();
    let qapi_pg = pg.clone();
    let qapi_auth = auth.clone();
    let qapi_socket = addrs.engine_socket.clone();
    let qapi_sd = sub(sd_rx);
    let qapi = tokio::spawn(async move {
        query_api::serve(
            &qapi_cfg,
            qapi_pg,
            qapi_auth,
            qapi_socket,
            qapi_listener,
            qapi_sd,
        )
        .await
    });

    // All listeners are bound and serving: signal composite readiness.
    let _sent = ready.send(());

    // Await all three; surface the first error.
    let (e, i, q) = tokio::join!(engine, ingest, qapi);
    let outcome = join_result(e).and(join_result(i)).and(join_result(q));

    // Stop the embedded PG last (after clients released their pool connections).
    if let Some(pg_handle) = pg_handle {
        pg_handle.shutdown().await?;
    }
    outcome
}

fn join_result(r: Result<Result<(), BoxErr>, tokio::task::JoinError>) -> Result<(), BoxErr> {
    match r {
        Ok(inner) => inner,
        Err(e) => Err(Box::new(e)),
    }
}
