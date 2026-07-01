//! The standalone composite: boot the embedded Postgres once and run engine
//! (tonic/UDS) + ingest (HTTP) + query-api (HTTP) as tasks in one runtime.
use std::future::Future;
use std::sync::Arc;

use control_plane_core::ControlPlane;
use tokio::task::{JoinError, JoinSet};

type BoxErr = Box<dyn std::error::Error + Send + Sync>;

/// The three listen addresses the composite binds.
pub struct StandaloneAddrs {
    pub query_api: std::net::SocketAddr,
    pub ingest: std::net::SocketAddr,
    pub engine_socket: String,
}

/// Boot the composite. Binds all listeners before spawning their serve loops so
/// readiness is race-free; `ready` fires once every listener is bound. On
/// `shutdown` — or as soon as any serve task exits — the servers are all stopped,
/// then the embedded PG is stopped last.
pub async fn run(
    cfg: service_runtime::Config,
    addrs: StandaloneAddrs,
    shutdown: impl Future<Output = ()> + Send + 'static,
    ready: tokio::sync::oneshot::Sender<()>,
) -> Result<(), BoxErr> {
    // Boot embedded PG (a no-op handle in external-DB mode). Owned here so PG is
    // stopped gracefully on EVERY exit of the composite below — clean shutdown,
    // a serve-task error, an engine-before-ready failure, or a startup bind error.
    let (pool, pg_handle) = service_runtime::build_pool_managed(&cfg).await?;
    let mut outcome = serve_composite(cfg, addrs, shutdown, ready, pool).await;
    stop_pg(pg_handle, &mut outcome).await;
    outcome
}

/// Run the three services over an already-booted control-plane `pool` until
/// `shutdown` fires or a serve task exits. PG lifecycle is the caller's concern
/// (see [`run`]); every error path here just returns, and the caller stops PG.
async fn serve_composite(
    cfg: service_runtime::Config,
    addrs: StandaloneAddrs,
    shutdown: impl Future<Output = ()> + Send + 'static,
    ready: tokio::sync::oneshot::Sender<()>,
    pool: sqlx::PgPool,
) -> Result<(), BoxErr> {
    // Shared singletons, built once.
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
    // The sender stays in this frame so BOTH an external shutdown signal AND the
    // first serve task to exit can fan shutdown out to the others (see the join).
    let (sd_tx, sd_rx) = tokio::sync::watch::channel(false);
    let sub = |rx: tokio::sync::watch::Receiver<bool>| async move {
        let mut rx = rx;
        while !*rx.borrow() {
            if rx.changed().await.is_err() {
                break;
            }
        }
    };

    // All three serve loops live in one JoinSet so we can react to whichever exits
    // first — a spontaneous error as well as a clean shutdown — instead of waiting
    // for all of them (a stuck-open `join!` was the error-cascade gap).
    let mut tasks: JoinSet<(&'static str, Result<(), BoxErr>)> = JoinSet::new();

    // Engine first: bind UDS synchronously, spawn, await engine-ready.
    drop(std::fs::remove_file(&addrs.engine_socket));
    let engine_listener = tokio::net::UnixListener::bind(&addrs.engine_socket)?;
    let (eng_ready_tx, eng_ready_rx) = tokio::sync::oneshot::channel();
    let engine_cfg = cfg.clone();
    let engine_pool = pool.clone();
    let engine_sd = sub(sd_rx.clone());
    tasks.spawn(async move {
        (
            "engine",
            engine::run(
                engine_listener,
                &engine_cfg,
                engine_pool,
                eng_ready_tx,
                engine_sd,
            )
            .await,
        )
    });

    // If the engine dies before signalling ready, surface its real error (not the
    // generic "exited before ready" string). The caller stops PG on this return.
    if eng_ready_rx.await.is_err() {
        return match tasks.join_next().await {
            Some(Ok((_, Err(e)))) => Err(format!("engine failed before ready: {e}").into()),
            Some(Err(e)) => Err(Box::new(e)),
            Some(Ok((_, Ok(())))) | None => Err("engine exited before signalling ready".into()),
        };
    }

    // Bind both HTTP listeners, then spawn their serves into the same set.
    let ingest_listener = tokio::net::TcpListener::bind(addrs.ingest).await?;
    let qapi_listener = tokio::net::TcpListener::bind(addrs.query_api).await?;

    let ingest_cfg = cfg.clone();
    let ingest_cp: Arc<dyn ControlPlane> = pg.clone();
    let ingest_auth = auth.clone();
    let ingest_pool = pool.clone();
    let ingest_admin = admin_subject.clone();
    let ingest_sd = sub(sd_rx.clone());
    tasks.spawn(async move {
        (
            "ingest",
            ingest::serve(
                &ingest_cfg,
                ingest_pool,
                ingest_cp,
                ingest_auth,
                ingest_admin,
                max_ttl,
                ingest_listener,
                ingest_sd,
            )
            .await,
        )
    });

    let qapi_cfg = cfg.clone();
    let qapi_pg = pg.clone();
    let qapi_auth = auth.clone();
    let qapi_socket = addrs.engine_socket.clone();
    let qapi_sd = sub(sd_rx);
    tasks.spawn(async move {
        (
            "query-api",
            query_api::serve(
                &qapi_cfg,
                qapi_pg,
                qapi_auth,
                qapi_socket,
                qapi_listener,
                qapi_sd,
            )
            .await,
        )
    });

    // All listeners are bound and serving: signal composite readiness.
    let _sent = ready.send(());

    // Run until an external shutdown OR the first serve task exits; then fan the
    // shutdown out to whatever is still serving and drain it. First error wins.
    let mut outcome: Result<(), BoxErr> = Ok(());
    tokio::select! {
        () = shutdown => {}
        Some(joined) = tasks.join_next() => record(&mut outcome, joined),
    }
    let _sent = sd_tx.send(true);
    while let Some(joined) = tasks.join_next().await {
        record(&mut outcome, joined);
    }
    outcome
}

/// Fold a finished serve task into the running outcome, keeping the first error
/// (labelled with the originating service). A clean exit and a panic are handled;
/// the first non-`Ok` wins so a later shutdown-triggered exit can't mask it.
fn record(
    outcome: &mut Result<(), BoxErr>,
    joined: Result<(&'static str, Result<(), BoxErr>), JoinError>,
) {
    let err: BoxErr = match joined {
        Ok((_name, Ok(()))) => return,
        Ok((name, Err(e))) => format!("{name}: {e}").into(),
        Err(e) => Box::new(e),
    };
    if outcome.is_ok() {
        *outcome = Err(err);
    }
}

/// Stop the embedded Postgres (a no-op in external-DB mode). A shutdown error is
/// surfaced only if the composite had no earlier error to report.
async fn stop_pg(
    pg_handle: Option<managed_postgres::EmbeddedPg>,
    outcome: &mut Result<(), BoxErr>,
) {
    if let Some(pg_handle) = pg_handle
        && let Err(e) = pg_handle.shutdown().await
        && outcome.is_ok()
    {
        *outcome = Err(e.into());
    }
}
