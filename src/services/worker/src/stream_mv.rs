//! The worker's micro-batch MV handler: fetch the source's framed offset delta
//! over Flight, derive the per-bucket watermark CAS bounds from the (gapless)
//! framing, strip the framing, run the standing query's SQL in a fresh
//! DataFusion session, and commit the result + watermark advance + run success
//! as ONE CommitMicroBatch RPC. Zero Postgres — the engine owns it.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use arrow::array::{Array, Int32Array, Int64Array, RecordBatch, StringArray};
use arrow::compute::concat_batches;
use arrow::datatypes::Schema;
use control_plane_core::{
    ControlPlaneError, DatasetRef, EventType, Job, JobFailure, LineageEvent, MAX_LOOKUP_KEYS,
    RunId, StreamMvJob, WatermarkAdvance, mv_key,
};
use datafusion::execution::context::SessionContext;
use datafusion_io::{infer_columns, register_batches};
use engine_wire::client::GrpcQueueClient;
use engine_wire::flight::{FlightTableClient, MvEnrichTicket};
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

        // Slice 5: derive the lookup-join key set from the delta BEFORE
        // `stripped` is moved into `register_batches` below. `None` when this
        // is a plain slice-4 MV (`job.enrich` unset) or a state-join
        // (`job.on` unset) or the delta's distinct key count exceeds
        // `MAX_LOOKUP_KEYS` (a superset full-state fetch is always correct).
        let key_payload: Option<(String, Vec<serde_json::Value>)> = if job.enrich.is_some() {
            match &job.on {
                None => None,
                Some(on) => {
                    let keys = distinct_lookup_keys(&stripped, &on.source_col)?;
                    (keys.len() <= MAX_LOOKUP_KEYS).then(|| (on.enrich_col.clone(), keys))
                }
            }
        } else {
            None
        };

        // Fresh session per job — no state leaks between micro-batches.
        let df_ctx = SessionContext::new();
        register_batches(&df_ctx, &job.source.name, schema, stripped)
            .map_err(|e| JobFailure::abandon(format!("register: {e}")))?;

        // Slice 5: register the enrich table's folded current state beside the
        // delta, under its own name — the join SQL references both tables.
        // Keyed (lookup-join) when `key_payload` is set, full state
        // (state-join) otherwise.
        if let Some(enrich) = &job.enrich {
            let enrich_batches = ctx
                .table
                .fetch_mv_enrich(MvEnrichTicket {
                    enrich_schema: enrich.schema.clone(),
                    enrich_name: enrich.name.clone(),
                    key: key_payload.as_ref().map(|(c, _)| c.clone()),
                    keys: key_payload.map(|(_, k)| k).unwrap_or_default(),
                })
                .await
                .map_err(|e| classify_enrich_error(&e, ctx.worker_tuning, attempts))?;
            match enrich_batches.first().map(RecordBatch::schema) {
                // The engine streams schema-first; a genuinely empty response
                // (the enrich table is live but has never had any data landed
                // — see `mv_enrich_scan`'s live-but-empty case) carries no
                // batch, and therefore no schema, over the wire (the Flight
                // encoder only emits a schema message when it sees a first
                // batch). Without a schema there is nothing safe to register
                // under `enrich.name`; the join SQL below then fails to
                // resolve that table, a deterministic abandon.
                None => {
                    return Err(JobFailure::abandon(format!(
                        "enrich: no schema available for empty enrich table {}.{}",
                        enrich.schema, enrich.name
                    )));
                }
                Some(enrich_schema) => {
                    register_batches(&df_ctx, &enrich.name, enrich_schema, enrich_batches)
                        .map_err(|e| JobFailure::abandon(format!("register enrich: {e}")))?;
                }
            }
        }

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
    // range this run consumed. Slice 5: a join MV's input set grows to
    // `[source, enrich]` and the payload gains an `"enrich"` (and `"on"` when
    // keyed) entry.
    let offsets: serde_json::Map<String, serde_json::Value> = advances
        .iter()
        .map(|a| {
            (
                a.bucket.to_string(),
                serde_json::json!({ "from": a.from, "to": a.to }),
            )
        })
        .collect();
    let mut inputs = vec![DatasetRef::from(&job.source)];
    let mut payload = serde_json::Map::new();
    payload.insert("sql".to_string(), serde_json::json!(job.sql));
    payload.insert("mv".to_string(), serde_json::json!(mv.clone()));
    payload.insert("offsets".to_string(), serde_json::Value::Object(offsets));
    if let Some(enrich) = &job.enrich {
        inputs.push(DatasetRef::from(enrich));
        payload.insert(
            "enrich".to_string(),
            serde_json::json!({ "schema": enrich.schema, "name": enrich.name }),
        );
        if let Some(on) = &job.on {
            payload.insert(
                "on".to_string(),
                serde_json::json!({ "source_col": on.source_col, "enrich_col": on.enrich_col }),
            );
        }
    }
    let lineage = LineageEvent {
        run_id: RunId(run_id.unwrap_or_else(uuid::Uuid::new_v4)),
        event_type: EventType::Complete,
        event_time: time::OffsetDateTime::now_utc(),
        inputs,
        outputs: vec![DatasetRef::from(&job.output)],
        payload: serde_json::Value::Object(payload),
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

/// Slice 5: the distinct, non-null values of `col` across `batches`, as JSON
/// scalars — the lookup-join's probe key set for `MvEnrichTicket.keys`. `col`
/// must downcast to `Int64Array`/`Int32Array`/`StringArray`; a missing column
/// or any other array type means the def's `LookupOn.source_col` doesn't match
/// the delta schema — malformed, deterministic, so Abandon.
fn distinct_lookup_keys(
    batches: &[RecordBatch],
    col: &str,
) -> std::result::Result<Vec<serde_json::Value>, JobFailure> {
    // A single small sum type so Int32/Int64/Utf8 keys share one `BTreeSet`
    // (distinctness/ordering, not cross-type comparison — a column is
    // homogeneously typed across every batch of one delta).
    #[derive(PartialEq, Eq, PartialOrd, Ord)]
    enum Key {
        Int(i64),
        Str(String),
    }

    let mut seen: BTreeSet<Key> = BTreeSet::new();
    for batch in batches {
        let idx = batch.schema().index_of(col).map_err(|e| {
            JobFailure::abandon(format!("lookup key: missing column '{col}' in delta: {e}"))
        })?;
        let array = batch.column(idx);
        if let Some(a) = array.as_any().downcast_ref::<Int64Array>() {
            for i in 0..a.len() {
                if !a.is_null(i) {
                    seen.insert(Key::Int(a.value(i)));
                }
            }
        } else if let Some(a) = array.as_any().downcast_ref::<Int32Array>() {
            for i in 0..a.len() {
                if !a.is_null(i) {
                    seen.insert(Key::Int(i64::from(a.value(i))));
                }
            }
        } else if let Some(a) = array.as_any().downcast_ref::<StringArray>() {
            for i in 0..a.len() {
                if !a.is_null(i) {
                    seen.insert(Key::Str(a.value(i).to_string()));
                }
            }
        } else {
            return Err(JobFailure::abandon(format!(
                "lookup key: column '{col}' is not Int64/Int32/Utf8 (got {:?})",
                array.data_type()
            )));
        }
    }
    Ok(seen
        .into_iter()
        .map(|k| match k {
            Key::Int(n) => serde_json::json!(n),
            Key::Str(s) => serde_json::json!(s),
        })
        .collect())
}

/// Slice 5: classify a `fetch_mv_enrich` wire error exactly like
/// `fetch_mv_delta`'s (`:74-84`): the client flattens gRPC status into an
/// opaque message, so only the message prefix survives — `"mv enrich:"` (the
/// engine's deterministic refusal, see `mv_enrich_scan`) is Abandon, every
/// other fault (wire/store) is Retry with the worker's backoff. Do NOT branch
/// on `tonic::Code` — it does not survive `FlightTableClient`'s flattening.
fn classify_enrich_error(
    e: &ControlPlaneError,
    worker_tuning: WorkerTuning,
    attempts: i32,
) -> JobFailure {
    let msg = e.to_string();
    if msg.contains("mv enrich:") {
        JobFailure::abandon(format!("fetch_mv_enrich: {msg}"))
    } else {
        JobFailure::retry(
            worker_tuning.backoff(attempts),
            format!("fetch_mv_enrich: {msg}"),
        )
    }
}
