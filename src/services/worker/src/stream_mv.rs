//! The worker's micro-batch MV handler: fetch the source's framed offset delta
//! over Flight, derive the per-bucket watermark CAS bounds from the (gapless)
//! framing, strip the framing, run the standing query's SQL in a fresh
//! DataFusion session, and commit the result + watermark advance + run success
//! as ONE CommitMicroBatch RPC. Zero Postgres — the engine owns it.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::{Array, Int32Array, Int64Array, RecordBatch};
use arrow::compute::concat_batches;
use arrow::datatypes::Schema;
use control_plane_core::{
    ControlPlaneError, DatasetRef, EventType, Job, JobFailure, LineageEvent, RunId, StreamMvJob,
    WatermarkAdvance, mv_key,
};
use datafusion::execution::context::SessionContext;
use datafusion_io::{infer_columns, register_batches};
use engine_wire::client::GrpcQueueClient;
use engine_wire::flight::FlightTableClient;
use engine_wire::pb;
use loom_config::WorkerTuning;

use crate::transform::{mark_running_if_tracked, report_run_failure};

#[derive(Clone)]
pub struct StreamMvCtx {
    pub control: GrpcQueueClient,
    /// Reads the standing query's source delta over the engine's Flight data
    /// plane (`MvDeltaTicket`) — cold Parquet UNION any still-live inline tail,
    /// framing columns included.
    pub table: FlightTableClient,
    pub worker_tuning: WorkerTuning,
}

/// Run one `"stream_mv"` job: one micro-batch of a standing query. Thin
/// lifecycle wrapper mirroring `handle_transform`: parse → (if `run_id`
/// present) mark the run Running → run the funnel → on failure, best-effort
/// report it against the run.
pub async fn handle_stream_mv(ctx: &StreamMvCtx, job: Job) -> std::result::Result<(), JobFailure> {
    let attempts = job.attempts;
    let parsed: StreamMvJob = serde_json::from_value(job.payload)
        .map_err(|e| JobFailure::abandon(format!("bad stream_mv payload: {e}")))?;
    let run_id = parsed.run_id;
    mark_running_if_tracked(&ctx.control, ctx.worker_tuning, run_id, attempts).await?;
    let result = run_stream_mv(ctx, attempts, &parsed, run_id).await;
    report_run_failure(&ctx.control, run_id, &result).await;
    result
}

async fn run_stream_mv(
    ctx: &StreamMvCtx,
    attempts: i32,
    job: &StreamMvJob,
    run_id: Option<uuid::Uuid>,
) -> std::result::Result<(), JobFailure> {
    let mv = mv_key(&job.output);

    // 2. Fetch the framed source delta. A deterministic refusal (unknown source
    //    table / source not a declared log stream table) is tagged `"mv delta:"`
    //    by the engine's serving read (`mv_delta_scan`) and mapped to
    //    `failed_precondition` on the wire — the only signal that survives the
    //    client's blanket `Backend` flattening is that message prefix, so key off
    //    it (mirrors the server's own `msg.starts_with("mv delta:")` dispatch).
    //    Every other wire fault is transient — retry.
    let batches = ctx
        .table
        .fetch_mv_delta(
            mv.clone(),
            job.source.schema.clone(),
            job.source.name.clone(),
        )
        .await
        .map_err(|e| {
            let msg = e.to_string();
            if msg.contains("mv delta:") {
                JobFailure::abandon(format!("fetch_mv_delta: {msg}"))
            } else {
                JobFailure::retry(
                    ctx.worker_tuning.backoff(attempts),
                    format!("fetch_mv_delta: {msg}"),
                )
            }
        })?;

    let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();

    // 3/4/6. An EMPTY delta (a debounced spurious wakeup) commits empty ipc +
    // empty advances — a cheap no-op that just marks the run succeeded. A
    // NON-empty delta always derives non-empty per-bucket advances from the
    // framing, even when the standing query's SQL filters every row: that
    // commit still carries the (non-empty) advances with an EMPTY ipc, so the
    // consumed delta is never reprocessed.
    let (columns, ipc, advances) = if total_rows == 0 {
        (Vec::new(), Vec::new(), Vec::new())
    } else {
        let advances = framing_bounds(&batches)?;
        let stripped = strip_framing(&batches)?;
        // Non-empty by construction: `stripped` mirrors `batches` 1:1, and
        // `batches` is non-empty here (`total_rows > 0`).
        let schema = stripped
            .first()
            .map(RecordBatch::schema)
            .ok_or_else(|| JobFailure::abandon("empty registration after strip"))?;

        // Fresh session per job — no state leaks between micro-batches.
        let df_ctx = SessionContext::new();
        register_batches(&df_ctx, &job.source.name, schema, stripped)
            .map_err(|e| JobFailure::abandon(format!("register: {e}")))?;

        let df = df_ctx
            .sql(&job.sql)
            .await
            .map_err(|e| JobFailure::abandon(format!("sql: {e}")))?;
        // Schema comes from the COMPILED SQL's result schema, not the wire —
        // the wire response carries no schema message on an empty delta, but
        // that case is already handled above.
        let out_schema: Arc<Schema> = Arc::new(df.schema().as_arrow().clone());
        let columns =
            infer_columns(&out_schema).map_err(|e| JobFailure::abandon(format!("infer: {e}")))?;
        let result_batches = df
            .collect()
            .await
            .map_err(|e| JobFailure::abandon(format!("collect: {e}")))?;
        let out_rows: usize = result_batches.iter().map(RecordBatch::num_rows).sum();
        let ipc = if out_rows == 0 {
            // A filtering micro-batch: the delta was non-empty but the SQL kept
            // nothing. Send EMPTY ipc with the NON-empty advances built above —
            // do not fold this into the empty-delta branch.
            Vec::new()
        } else {
            encode_ipc_stream(&out_schema, &result_batches)
                .map_err(|e| JobFailure::abandon(format!("encode: {e}")))?
        };
        (columns, ipc, advances)
    };

    // 7. Lineage event: inputs/outputs name the physical tables; the payload
    // records the compiled SQL, the watermark key, and the per-bucket offset
    // range this run consumed.
    let offsets: serde_json::Map<String, serde_json::Value> = advances
        .iter()
        .map(|a| {
            (
                a.bucket.to_string(),
                serde_json::json!({ "from": a.from, "to": a.to }),
            )
        })
        .collect();
    let lineage = LineageEvent {
        run_id: RunId(run_id.unwrap_or_else(uuid::Uuid::new_v4)),
        event_type: EventType::Complete,
        event_time: time::OffsetDateTime::now_utc(),
        inputs: vec![DatasetRef::from(&job.source)],
        outputs: vec![DatasetRef::from(&job.output)],
        payload: serde_json::json!({
            "sql": job.sql,
            "mv": mv.clone(),
            "offsets": offsets,
        }),
    };

    let pb_advances: Vec<pb::MvAdvance> = advances
        .into_iter()
        .map(|a| pb::MvAdvance {
            bucket: a.bucket,
            from: a.from,
            to: a.to,
        })
        .collect();

    // 8. Commit atomically over the wire: output + watermark advance + run
    // success, one RPC. `Ok(None)` (an empty micro-batch — no rows, no
    // advances, or a filtering micro-batch) is a normal outcome, not a failure.
    ctx.control
        .commit_micro_batch(
            mv,
            job.source.schema.clone(),
            job.source.name.clone(),
            job.output.schema.clone(),
            job.output.name.clone(),
            job.buckets,
            &columns,
            ipc,
            &lineage,
            pb_advances,
            run_id,
        )
        .await
        .map_err(|e| match e {
            // A stale watermark CAS: a newer run already covered this delta —
            // deterministic, never retry (a retry would race the same conflict).
            ControlPlaneError::Conflict(m) => JobFailure::abandon(format!(
                "superseded: watermark advanced concurrently (a newer run covers this delta): {m}"
            )),
            ControlPlaneError::Validation(m) => {
                JobFailure::abandon(format!("commit_micro_batch refused: {m}"))
            }
            other => JobFailure::retry(
                ctx.worker_tuning.backoff(attempts),
                format!("commit_micro_batch: {other}"),
            ),
        })?;
    Ok(())
}

/// Locate the `loom_bucket`/`loom_offset` framing columns and fold every
/// batch's rows into a per-bucket `(min, max)` offset range. The delta is
/// gapless per bucket from the committed watermark, so the observed minimum
/// offset for a bucket IS the watermark's `from` bound; `to` is one past the
/// observed maximum (the CAS's exclusive upper bound, matching
/// `WatermarkAdvance`'s documented contract). A missing/mistyped framing
/// column is a malformed delta — deterministic, so Abandon.
fn framing_bounds(
    batches: &[RecordBatch],
) -> std::result::Result<Vec<WatermarkAdvance>, JobFailure> {
    let mut bounds: BTreeMap<i32, (i64, i64)> = BTreeMap::new();
    for batch in batches {
        let schema = batch.schema();
        let bucket_idx = schema.index_of("loom_bucket").map_err(|e| {
            JobFailure::abandon(format!("malformed delta: missing loom_bucket column: {e}"))
        })?;
        let offset_idx = schema.index_of("loom_offset").map_err(|e| {
            JobFailure::abandon(format!("malformed delta: missing loom_offset column: {e}"))
        })?;
        let bucket_col = batch
            .column(bucket_idx)
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or_else(|| JobFailure::abandon("malformed delta: loom_bucket is not Int32"))?;
        let offset_col = batch
            .column(offset_idx)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| JobFailure::abandon("malformed delta: loom_offset is not Int64"))?;
        for i in 0..batch.num_rows() {
            let b = bucket_col.value(i);
            let o = offset_col.value(i);
            bounds
                .entry(b)
                .and_modify(|(min, max)| {
                    *min = (*min).min(o);
                    *max = (*max).max(o);
                })
                .or_insert((o, o));
        }
    }
    Ok(bounds
        .into_iter()
        .map(|(bucket, (min, max))| WatermarkAdvance {
            bucket,
            from: min,
            to: max + 1,
        })
        .collect())
}

/// Project out every `loom_`-prefixed framing column from each batch, keeping
/// the user columns in their declared order.
fn strip_framing(batches: &[RecordBatch]) -> std::result::Result<Vec<RecordBatch>, JobFailure> {
    batches
        .iter()
        .map(|b| {
            let keep: Vec<usize> = b
                .schema()
                .fields()
                .iter()
                .enumerate()
                .filter(|(_, f)| !f.name().starts_with("loom_"))
                .map(|(i, _)| i)
                .collect();
            b.project(&keep)
                .map_err(|e| JobFailure::abandon(format!("strip framing: {e}")))
        })
        .collect()
}

/// Concat `batches` (all sharing `schema`) and encode as one Arrow IPC stream
/// — the `write_delta` client-side convention (single-batch, schema-first).
fn encode_ipc_stream(
    schema: &Schema,
    batches: &[RecordBatch],
) -> std::result::Result<Vec<u8>, arrow::error::ArrowError> {
    let schema_ref = Arc::new(schema.clone());
    let batch = concat_batches(&schema_ref, batches)?;
    let mut buf = Vec::new();
    {
        let mut w = arrow::ipc::writer::StreamWriter::try_new(&mut buf, &batch.schema())?;
        w.write(&batch)?;
        w.finish()?;
    }
    Ok(buf)
}
