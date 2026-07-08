//! The worker's stream-consolidate job handler: a thin trigger that decodes the
//! `stream_consolidate` payload and issues the engine's `ConsolidateStream` RPC
//! (fold a CDC table's base by LastRow-per-identity, clear its `has_shadow`
//! flag — all state and compute live on the engine side, Task 6). One RPC, so
//! this reuses `handler::run_wire_job`'s parse-then-RPC shape rather than
//! introducing a new per-job ctx: the wire client is the whole dependency,
//! exactly like `handle_flush`/`handle_gc`/`handle_build_vector_index`.

use control_plane_core::{Job, JobFailure, StreamConsolidateJob};
use engine_wire::client::GrpcQueueClient;
use loom_config::WorkerTuning;

use crate::handler::run_wire_job;

pub async fn handle_stream_consolidate(
    engine: GrpcQueueClient,
    tuning: WorkerTuning,
    job: Job,
) -> std::result::Result<(), JobFailure> {
    run_wire_job(
        job,
        tuning,
        "stream_consolidate",
        |StreamConsolidateJob { schema, name }| async move {
            engine.consolidate_stream(schema, name).await.map(|_| ())
        },
    )
    .await
}
