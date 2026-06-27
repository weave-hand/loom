//! The worker's compaction job handler: list a table's live files over the wire,
//! pick the small ones, stream them via Flight, rewrite coalesced to object store,
//! and commit the swap over CompactTable. Zero Postgres — the engine owns it.
use std::sync::Arc;

use control_plane_core::{CompactJob, DataFile, Job, JobFailure, RetryPolicy};
use datafusion_io::{WriteConfig, absolute_data_files, write_dataset};
use engine_wire::client::GrpcQueueClient;
use engine_wire::flight::{FlightTableClient, FlightTicket};
use loom_config::WorkerTuning;
use store_config::WriteStore;
use transform::small_files;

#[derive(Clone)]
pub struct CompactCtx {
    pub control: GrpcQueueClient,
    pub flight: FlightTableClient,
    pub write: Arc<WriteStore>,
    pub threshold_bytes: i64,
    pub write_cfg: WriteConfig,
    pub worker_tuning: WorkerTuning,
}

fn retry(tuning: &WorkerTuning, attempts: i32, error: String) -> JobFailure {
    JobFailure {
        error,
        policy: RetryPolicy::Retry {
            delay: tuning.backoff(attempts),
        },
    }
}

pub async fn handle_compact(ctx: &CompactCtx, job: Job) -> std::result::Result<(), JobFailure> {
    let attempts = job.attempts;
    let CompactJob { schema, name } =
        serde_json::from_value(job.payload).map_err(|e| JobFailure {
            error: format!("bad compact payload: {e}"),
            policy: RetryPolicy::Abandon,
        })?;

    let live = ctx
        .control
        .list_files(schema.clone(), name.clone())
        .await
        .map_err(|e| retry(&ctx.worker_tuning, attempts, format!("list_files: {e}")))?;
    let small = small_files(&live, ctx.threshold_bytes);
    if small.len() < 2 {
        return Ok(()); // no-op: nothing worth coalescing (converges).
    }
    let small_paths: Vec<String> = small.iter().map(|f| f.path.clone()).collect();

    let batches = ctx
        .flight
        .fetch(FlightTicket {
            schema: schema.clone(),
            name: name.clone(),
            files: small_paths.clone(),
        })
        .await
        .map_err(|e| retry(&ctx.worker_tuning, attempts, format!("flight fetch: {e}")))?;
    let Some(first) = batches.first() else {
        return Ok(());
    };
    let arrow_schema = first.schema();
    let run_id = uuid::Uuid::new_v4().to_string();
    let dir_prefix = format!("{schema}/{name}/{run_id}");
    let written = write_dataset(
        ctx.write.store.clone(),
        &dir_prefix,
        arrow_schema,
        &batches,
        &ctx.write_cfg,
    )
    .await
    .map_err(|e| retry(&ctx.worker_tuning, attempts, format!("write_dataset: {e}")))?;

    let new_files: Vec<DataFile> =
        absolute_data_files(written, &ctx.write.root_url, &schema, &name);

    ctx.control
        .compact_table(schema, name, small_paths, &new_files)
        .await
        .map_err(|e| retry(&ctx.worker_tuning, attempts, format!("compact_table: {e}")))?;
    Ok(())
}
