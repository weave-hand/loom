//! The standalone composite: boot the embedded Postgres once and run engine
//! (tonic/UDS) + ingest (HTTP) + query-api (HTTP) + the queue worker as tasks in
//! one runtime. Without the worker nothing drains the queue, so flush, GC,
//! compaction, transforms and micro-batch MVs would never run.
use std::future::Future;
use std::sync::Arc;

use control_plane_core::ControlPlane;
use tokio::task::{JoinError, JoinSet};
use tokio_util::sync::CancellationToken;

type BoxErr = Box<dyn std::error::Error + Send + Sync>;

/// The three listen addresses the composite binds.
pub struct StandaloneAddrs {
    pub query_api: std::net::SocketAddr,
    pub ingest: std::net::SocketAddr,
    pub engine_socket: String,
}

/// Env-derived tunables the composite passes to its services: the auth TTLs, the
/// engine write-path byte thresholds, and the in-process worker's job config.
/// Parsed once from the main's env snapshot (fail-loud on malformed values) and
/// handed into [`run`]; the composite itself never reads the live environment.
///
/// Not `Copy`/`Eq`: `jobs.write` is a `WriteConfig`, which holds an `f64` and is
/// `Clone`-only.
#[derive(Clone, Debug)]
pub struct StandaloneTuning {
    pub session_ttl: std::time::Duration,
    pub max_ttl: std::time::Duration,
    pub lockout: service_runtime::LockoutPolicy,
    pub engine: engine::EngineTuning,
    /// Worker loop + write-path config for the in-process worker. Composed
    /// defaults < file < env, exactly as `worker-bin` does it.
    pub jobs: datafusion_io::JobConfig,
    /// Compaction size threshold, `LOOM_COMPACT_THRESHOLD_BYTES` (default 128 MiB).
    /// Mirrors `src/services/worker/src/main.rs:53-54`.
    pub compact_threshold_bytes: i64,
}

impl StandaloneTuning {
    /// Parse from the env snapshot. Absent keys take the documented defaults
    /// (24h session TTL, 90-day token cap, 16 MiB inline / 64 MiB flush,
    /// 5s worker poll, 128 MiB compaction threshold).
    pub fn from_map(
        vars: &std::collections::HashMap<String, String>,
    ) -> Result<Self, service_runtime::ConfigError> {
        let mut compact_threshold_bytes: i64 = 128 * 1024 * 1024;
        service_runtime::overlay_opt(
            vars,
            "LOOM_COMPACT_THRESHOLD_BYTES",
            &mut compact_threshold_bytes,
        )?;
        Ok(StandaloneTuning {
            session_ttl: service_runtime::session_ttl(vars)?,
            max_ttl: service_runtime::service_token_max_ttl(vars)?,
            lockout: service_runtime::login_lockout(vars)?,
            engine: engine::EngineTuning::from_map(vars)?,
            jobs: service_runtime::load(vars)?,
            compact_threshold_bytes,
        })
    }
}

/// Boot the composite. Binds all listeners before spawning their serve loops so
/// readiness is race-free; `ready` fires once every listener is bound. On
/// `shutdown` — or as soon as any serve task exits — the servers are all stopped,
/// then the embedded PG is stopped last.
pub async fn run(
    cfg: service_runtime::Config,
    addrs: StandaloneAddrs,
    tuning: StandaloneTuning,
    shutdown: impl Future<Output = ()> + Send + 'static,
    ready: tokio::sync::oneshot::Sender<()>,
) -> Result<(), BoxErr> {
    // Boot embedded PG (a no-op handle in external-DB mode). Owned here so PG is
    // stopped gracefully on EVERY exit of the composite below — clean shutdown,
    // a serve-task error, an engine-before-ready failure, or a startup bind error.
    let (pool, pg_handle) = service_runtime::build_pool_managed(&cfg).await?;
    let mut outcome = serve_composite(cfg, addrs, tuning, shutdown, ready, pool).await;
    stop_pg(pg_handle, &mut outcome).await;
    outcome
}

/// Run the services (engine, ingest, query-api, worker) over an already-booted
/// control-plane `pool` until `shutdown` fires or a serve task exits. PG lifecycle
/// is the caller's concern (see [`run`]); every error path here just returns, and
/// the caller stops PG.
async fn serve_composite(
    cfg: service_runtime::Config,
    addrs: StandaloneAddrs,
    tuning: StandaloneTuning,
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
        session_ttl: tuning.session_ttl,
        lockout: tuning.lockout,
    };
    let max_ttl = tuning.max_ttl;
    // `StandaloneTuning` is no longer `Copy` (it carries a `WriteConfig`), so take
    // the engine's tuning out before the spawn below moves `tuning`.
    // `engine::EngineTuning` IS `Copy`, so this is a copy.
    let engine_tuning = tuning.engine;

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

    // Every serve loop lives in one JoinSet so we can react to whichever exits
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
                engine_tuning,
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

    // Worker: dials the engine we just brought up, over the same UDS query-api uses.
    // The write store comes from `cfg.object_store` — already resolved by
    // `Config::from_map` with its LOOM_DATA_PATH fallback — rather than
    // `ObjectStoreConfig::parse_from_env`, which REQUIRES LOOM_WAREHOUSE_URI. The
    // deployed `loom` binary's embedded mode sets only LOOM_DATA_PATH, so re-parsing
    // the env here would fail on exactly the default single-binary configuration.
    let worker_rt = worker::runtime::WorkerRuntime {
        socket: addrs.engine_socket.clone(),
        // A fresh id per process: the composite never reads the live environment,
        // so LOOM_WORKER_ID is deliberately not consulted.
        worker_id: uuid::Uuid::new_v4().to_string(),
        lease: cfg.lock_timeout,
        write: Arc::new(service_runtime::build_write_store(&cfg.object_store)?),
        jobs: tuning.jobs.clone(),
        compact_threshold_bytes: tuning.compact_threshold_bytes,
    };

    // Bridge the composite's watch-channel shutdown to the worker's CancellationToken.
    let worker_cancel = CancellationToken::new();
    let cancel_src = worker_cancel.clone();
    let worker_sd = sub(sd_rx.clone());
    tokio::spawn(async move {
        worker_sd.await;
        cancel_src.cancel();
    });

    tasks.spawn(async move {
        (
            "worker",
            worker::runtime::run_worker(worker_rt, worker_cancel)
                .await
                .map_err(Into::into),
        )
    });

    // Bind both HTTP listeners, then spawn their serves into the same set.
    let ingest_listener = tokio::net::TcpListener::bind(addrs.ingest).await?;
    let qapi_listener = tokio::net::TcpListener::bind(addrs.query_api).await?;

    let ingest_cfg = cfg.clone();
    let ingest_cp: Arc<dyn ControlPlane> = pg.clone();
    let ingest_auth = auth.clone();
    let ingest_pool = pool.clone();
    let ingest_sd = sub(sd_rx.clone());
    tasks.spawn(async move {
        (
            "ingest",
            ingest::serve(
                &ingest_cfg,
                ingest_pool,
                ingest_cp,
                ingest_auth,
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
