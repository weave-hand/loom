#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic,
    clippy::let_underscore_must_use,
    clippy::unused_result_ok,
    clippy::map_err_ignore,
    clippy::unreachable,
    clippy::partial_pub_fields,
    reason = "cross-crate test/fixture harness code, not a production path; \
              EngineGuard deliberately keeps its socket TempDir private"
)]
//! Engine-over-UDS spawn harness (`road-test-wire-harness`).
//!
//! One `spawn_engine_uds(fx, db, warehouse, EngineOpts)` replaces the tree's
//! copies of `spawn_flight` (engine + query-api tests), `spawn_server`
//! (worker tests), and e2e-support's `spawn_engine` — with **connect-retry
//! readiness** instead of the `sleep(20ms)`-and-hope sync those copies used
//! (the tree's classic flake source). The guard owns the socket dir and the
//! serve task; hold it alive for the duration of the test.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use arrow_flight::flight_service_server::FlightServiceServer;
use control_plane_postgres::PgControlPlane;
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use engine::flight::FlightDataService;
use engine::service::EngineControlService;
use engine_serving::IcebergActionWriter;
use engine_wire::pb::engine_control_server::EngineControlServer;
use loom_test_seed::local_sql_catalog;
use store_config::{ObjectStoreConfig, build_write_store};
use tokio_stream::wrappers::UnixListenerStream;
use tonic::transport::Server;

/// Which services the spawned engine serves, and the action-writer limits.
/// Defaults mirror the tree's most common spawn: Flight only, 16 MiB inline
/// limit, no flush threshold.
pub struct EngineOpts {
    /// Serve `EngineControl` (queue/commit RPCs).
    pub control: bool,
    /// Serve Arrow Flight (`FlightDataService`).
    pub flight: bool,
    /// `IcebergActionWriter` inline byte limit (control plane writer).
    pub inline_byte_limit: usize,
    /// `IcebergActionWriter` flush byte threshold.
    pub flush_byte_threshold: i64,
    /// Grace window for the engine's orphan sweep RPC.
    pub orphan_sweep_grace: Duration,
}

impl Default for EngineOpts {
    fn default() -> Self {
        Self {
            control: false,
            flight: true,
            inline_byte_limit: 16 * 1024 * 1024,
            flush_byte_threshold: i64::MAX,
            orphan_sweep_grace: Duration::from_secs(24 * 3600),
        }
    }
}

/// Keep-alive guard for a spawned engine: socket path, serve-task handle,
/// and the owned socket TempDir. Dropping it aborts the serve task and
/// tears the socket dir down.
pub struct EngineGuard {
    /// Filesystem path of the bound unix socket.
    pub sock: String,
    /// The tokio task running the tonic server.
    pub handle: tokio::task::JoinHandle<()>,
    _sock_dir: tempfile::TempDir,
}

impl Drop for EngineGuard {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

/// Build a writable `WriteStore` rooted at a local `warehouse` dir (test
/// helper): the same `file://`-warehouse shape production uses, for standing
/// up an `EngineControlService` in tests. Panics on a bad config — test-only.
pub fn test_write_store(warehouse: &str) -> store_config::WriteStore {
    let mut env = HashMap::new();
    env.insert(
        "LOOM_WAREHOUSE_URI".to_string(),
        format!("file://{warehouse}"),
    );
    let store_cfg = ObjectStoreConfig::parse_from_env(&env).expect("store config");
    build_write_store(&store_cfg).expect("write store")
}

/// Spawn an engine on a fresh UDS serving the services `opts` selects,
/// backed by `db` + `warehouse`. Returns only after a client can connect
/// (connect-retry readiness — no fixed sleep).
pub async fn spawn_engine_uds(
    fx: &PgFixture,
    db: &str,
    warehouse: &str,
    opts: EngineOpts,
) -> EngineGuard {
    assert!(opts.control || opts.flight, "spawn at least one service");
    let sock_dir = tempfile::tempdir().expect("socket dir");
    let sock_path = sock_dir.path().join("engine.sock");
    let sock = sock_path.to_string_lossy().to_string();
    let pool = fx.pool_for(db).await;
    // One shared catalog, mirroring production `engine::run::run`.
    let catalog = Arc::new(local_sql_catalog(fx.pg_dsn(db), warehouse).await);
    // One shared control-plane handle, cloned into each service that needs it
    // (mirrors production `engine::run::run`'s single `cp` cloned into both
    // `EngineControlService` and `FlightDataService`).
    let cp = PgControlPlane::new(pool.clone(), Duration::from_millis(5000));

    let control = opts.control.then(|| {
        let cp = cp.clone();
        let writer = IcebergActionWriter::new(
            catalog.clone(),
            pool.clone(),
            opts.inline_byte_limit,
            opts.flush_byte_threshold,
        );
        let write_store = test_write_store(warehouse);
        EngineControlServer::new(EngineControlService {
            cp,
            catalog: catalog.clone(),
            pool: pool.clone(),
            retention: Duration::from_secs(7 * 24 * 3600),
            writer,
            flush_byte_threshold: opts.flush_byte_threshold,
            write_store,
            orphan_sweep_grace: opts.orphan_sweep_grace,
            serving_store: None,
        })
    });
    let flight = opts.flight.then(|| {
        FlightServiceServer::new(FlightDataService {
            catalog,
            serving_catalog: IcebergCatalog::new(pool.clone()),
            serving_store: None,
            pool,
            cp,
            sql_limits: engine_serving::GovernedSqlLimits::unbounded(),
            sql_admission: engine::flight::GovernedSqlAdmission::new(
                16,
                Duration::from_secs(5),
            ),
        })
    });

    let listener = tokio::net::UnixListener::bind(&sock_path).expect("bind uds");
    let incoming = UnixListenerStream::new(listener);
    let handle = tokio::spawn(async move {
        drop(
            Server::builder()
                .add_optional_service(control)
                .add_optional_service(flight)
                .serve_with_incoming(incoming)
                .await,
        );
    });
    await_uds_ready(&sock_path).await;

    EngineGuard {
        sock,
        handle,
        _sock_dir: sock_dir,
    }
}

/// The classic Flight-only spawn (the 5 `spawn_flight` copies).
pub async fn spawn_flight_uds(fx: &PgFixture, db: &str, warehouse: &str) -> EngineGuard {
    spawn_engine_uds(fx, db, warehouse, EngineOpts::default()).await
}

/// Poll-connect until the UDS accepts (≤ 5s), replacing fixed-sleep syncs.
async fn await_uds_ready(path: &Path) {
    for _ in 0..250 {
        if tokio::net::UnixStream::connect(path).await.is_ok() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("engine UDS not ready after 5s: {}", path.display());
}
