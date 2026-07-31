//! The engine service seam: build the control + flight services over a shared
//! pool and serve them on a pre-bound Unix socket until `shutdown` resolves.
use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use arrow_flight::flight_service_server::FlightServiceServer;
use control_plane_core::ControlPlane;
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use control_plane_postgres::iceberg_compact::CompactTriggerCfg;
use control_plane_postgres::iceberg_sql_catalog::{
    SQL_CATALOG_PROP_URI, SQL_CATALOG_PROP_WAREHOUSE, SqlCatalogBuilder,
};
use iceberg::CatalogBuilder;
use tokio::net::UnixListener;
use tokio::sync::oneshot;
use tokio_stream::wrappers::UnixListenerStream;
use tonic::transport::Server;

use engine_wire::pb::engine_control_server::EngineControlServer;

use crate::flight::FlightDataService;
use crate::scheduler;
use crate::service::EngineControlService;

type BoxErr = Box<dyn std::error::Error + Send + Sync>;

/// Engine write-path byte thresholds, parsed once from the caller's env snapshot
/// (the engine main / the standalone composite) — `run` itself never reads the
/// live environment (one-env-snapshot-per-main).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EngineTuning {
    /// Row payloads at/below this size commit as inline PG rows
    /// (`LOOM_INLINE_BYTE_LIMIT`, default 16 MiB).
    pub inline_byte_limit: usize,
    /// Inline-row bytes above which a flush-to-Parquet job is enqueued
    /// (`LOOM_FLUSH_BYTE_THRESHOLD`, default 64 MiB).
    pub flush_byte_threshold: i64,
    /// Accumulated CDC delta-row count (per declared stream table) at/above which
    /// a `stream_consolidate` job is enqueued (`LOOM_CONSOLIDATE_DELTA_THRESHOLD`,
    /// default 128 — mirrors `ingest::config::RoutingTuning::consolidate_delta_threshold`).
    pub consolidate_delta_threshold: i64,
    /// How often the scheduler loop claims due transform schedules
    /// (`LOOM_SCHEDULER_TICK_SECS`, default 5).
    pub scheduler_tick: Duration,
    /// How often the reconciliation loop sweeps stranded runs
    /// (`LOOM_RECONCILE_TICK_SECS`, default 60).
    pub reconcile_tick: Duration,
    /// Minimum age of a `Running` run before it is eligible for reconciliation
    /// (`LOOM_RECONCILE_GRACE_SECS`, default 120).
    pub reconcile_grace: Duration,
    /// Small-file cutoff for the compaction auto-trigger — the same env the
    /// worker's `small_files` selection reads, so one deploy value governs both
    /// (`LOOM_COMPACT_THRESHOLD_BYTES`, default 128 MiB).
    pub compact_small_file_bytes: i64,
    /// Number of small files (at/above `compact_small_file_bytes`) that must
    /// accumulate before a compaction job is auto-enqueued
    /// (`LOOM_COMPACT_TRIGGER_FILES`, default 8). `0` disables the trigger;
    /// `1` is rejected at startup (would re-enqueue immediately after every
    /// compaction whose output stays under the cutoff); `>= 2` enables.
    pub compact_trigger_files: i64,
}

impl EngineTuning {
    /// Parse from the env snapshot: absent keys take the defaults, a
    /// present-but-malformed value fails startup naming the key.
    pub fn from_map(vars: &HashMap<String, String>) -> Result<Self, service_runtime::ConfigError> {
        let tuning = EngineTuning {
            inline_byte_limit: service_runtime::parse_var(
                vars,
                "LOOM_INLINE_BYTE_LIMIT",
                16 * 1024 * 1024,
            )?,
            flush_byte_threshold: service_runtime::parse_var(
                vars,
                "LOOM_FLUSH_BYTE_THRESHOLD",
                64 * 1024 * 1024,
            )?,
            consolidate_delta_threshold: service_runtime::parse_var(
                vars,
                "LOOM_CONSOLIDATE_DELTA_THRESHOLD",
                128_i64,
            )?,
            scheduler_tick: Duration::from_secs(service_runtime::parse_var(
                vars,
                "LOOM_SCHEDULER_TICK_SECS",
                5_u64,
            )?),
            reconcile_tick: Duration::from_secs(service_runtime::parse_var(
                vars,
                "LOOM_RECONCILE_TICK_SECS",
                60_u64,
            )?),
            reconcile_grace: Duration::from_secs(service_runtime::parse_var(
                vars,
                "LOOM_RECONCILE_GRACE_SECS",
                120_u64,
            )?),
            compact_small_file_bytes: service_runtime::parse_var(
                vars,
                "LOOM_COMPACT_THRESHOLD_BYTES",
                128 * 1024 * 1024,
            )?,
            compact_trigger_files: service_runtime::parse_var(
                vars,
                "LOOM_COMPACT_TRIGGER_FILES",
                8_i64,
            )?,
        };
        validate_compact_trigger_files(tuning.compact_trigger_files)?;
        if tuning.compact_small_file_bytes <= 0 {
            return Err(service_runtime::invalid(
                "LOOM_COMPACT_THRESHOLD_BYTES",
                "must be >= 1",
            ));
        }
        Ok(tuning)
    }
}

/// Reject `LOOM_COMPACT_TRIGGER_FILES == 1` (and negatives) after parsing: `0`
/// disables the trigger and any value `>= 2` enables it, but `1` would
/// re-enqueue a compaction job immediately after every prior compaction whose
/// output stays under the cutoff.
fn validate_compact_trigger_files(n: i64) -> Result<(), service_runtime::ConfigError> {
    if n == 1 || n < 0 {
        return Err(service_runtime::invalid(
            "LOOM_COMPACT_TRIGGER_FILES",
            "must be 0 or >= 2",
        ));
    }
    Ok(())
}

/// Build and serve the engine on `listener`. Fires `ready` once the services are
/// built and the serve loop is about to run; returns when `shutdown` resolves.
pub async fn run(
    listener: UnixListener,
    cfg: &service_runtime::Config,
    pool: sqlx::PgPool,
    tuning: EngineTuning,
    ready: oneshot::Sender<()>,
    shutdown: impl Future<Output = ()> + Send,
) -> Result<(), BoxErr> {
    let cp = service_runtime::control_plane(pool.clone(), cfg.lock_timeout);
    let sched_cp: Arc<dyn ControlPlane> = Arc::new(cp.clone());

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
    let catalog = if tuning.compact_trigger_files >= 2 {
        catalog.with_compact_trigger(CompactTriggerCfg {
            small_file_bytes: tuning.compact_small_file_bytes,
            min_small_files: tuning.compact_trigger_files,
        })
    } else {
        catalog
    };
    let catalog = std::sync::Arc::new(catalog);
    let writer = engine_serving::IcebergActionWriter::new(
        catalog.clone(),
        pool.clone(),
        tuning.inline_byte_limit,
        tuning.flush_byte_threshold,
    )
    .with_consolidate_delta_threshold(tuning.consolidate_delta_threshold);
    let write_store = service_runtime::build_write_store(&cfg.object_store)?;

    let serving_store = service_runtime::build_serving_object_store(&cfg.object_store)?;
    let control = EngineControlService {
        cp: cp.clone(),
        catalog: catalog.clone(),
        pool: pool.clone(),
        retention: cfg.gc_retention,
        writer,
        flush_byte_threshold: tuning.flush_byte_threshold,
        write_store,
        orphan_sweep_grace: cfg.orphan_sweep_grace,
        serving_store: serving_store.clone(),
    };
    let flight = FlightDataService {
        catalog,
        serving_catalog: IcebergCatalog::new(pool.clone()),
        serving_store,
        pool,
        cp,
    };

    // Signal readiness: the caller binds `listener` before spawning us, so the
    // socket already accepts; this tells the caller the serve loop is starting.
    // The send fails only if the receiver dropped (e.g. throwaway in main.rs), which is fine.
    let _sent = ready.send(());

    let loops = scheduler::BackgroundLoops::spawn(sched_cp, &tuning);

    let served = Server::builder()
        .add_service(EngineControlServer::new(control))
        .add_service(FlightServiceServer::new(flight))
        .serve_with_incoming_shutdown(UnixListenerStream::new(listener), shutdown)
        .await;

    // Stop the loops on EVERY exit — a serve error previously skipped both cancels,
    // leaving them ticking against the pool while the process tore down.
    loops.stop().await;
    served?;
    Ok(())
}
