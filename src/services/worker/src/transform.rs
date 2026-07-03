//! The worker's transform job handler: resolve each input's live files + declared
//! schema over the wire (ListFiles), stream the rows via Flight, run the job's SQL
//! in a fresh DataFusion session, write the result as Parquet to object store, and
//! commit it atomically over CommitTransform (files + lineage, append or replace).
//! Zero Postgres — the engine owns it.
use std::collections::HashSet;
use std::sync::Arc;

use control_plane_core::{
    DatasetRef, EventType, Job, JobFailure, LineageEvent, OutputMode, PropertyDef, RunId, TableRef,
    TransformJob, check_conformance,
};
use datafusion::execution::context::SessionContext;
use datafusion_io::{
    WriteConfig, absolute_data_files, infer_columns, logical_arrow_schema, register_batches,
    register_empty_table, write_dataset,
};
use engine_wire::client::GrpcQueueClient;
use engine_wire::flight::{FlightTableClient, FlightTicket};
use loom_config::WorkerTuning;
use store_config::WriteStore;

#[derive(Clone)]
pub struct TransformCtx {
    pub control: GrpcQueueClient,
    pub flight: FlightTableClient,
    pub write: Arc<WriteStore>,
    pub write_cfg: WriteConfig,
    pub worker_tuning: WorkerTuning,
}

/// Run one physical `"transform"` job: SQL over table-named inputs, each registered
/// under its own table name.
pub async fn handle_transform(ctx: &TransformCtx, job: Job) -> std::result::Result<(), JobFailure> {
    let attempts = job.attempts;
    let parsed: TransformJob = serde_json::from_value(job.payload)
        .map_err(|e| JobFailure::abandon(format!("bad transform payload: {e}")))?;
    let inputs: Vec<(String, TableRef)> = parsed
        .inputs
        .iter()
        .map(|t| (t.name.clone(), t.clone()))
        .collect();
    let lineage = LineageEvent {
        run_id: RunId(uuid::Uuid::new_v4()),
        event_type: EventType::Complete,
        event_time: time::OffsetDateTime::now_utc(),
        inputs: parsed.inputs.iter().map(DatasetRef::from).collect(),
        outputs: vec![DatasetRef::from(&parsed.output)],
        payload: serde_json::json!({ "sql": parsed.sql }),
    };
    run_wire_transform(
        ctx,
        attempts,
        WireTransform {
            inputs,
            output: &parsed.output,
            sql: &parsed.sql,
            conform: None,
            output_mode: parsed.output_mode,
            lineage,
        },
    )
    .await
}

/// One wire transform, shared by the physical and typed handlers: `inputs` are
/// `(register_as, table)` pairs (the typed path registers under the ontology type
/// name), `conform` is the typed path's exact-match contract (`None` skips it).
struct WireTransform<'a> {
    inputs: Vec<(String, TableRef)>,
    output: &'a TableRef,
    sql: &'a str,
    conform: Option<&'a [PropertyDef]>,
    output_mode: OutputMode,
    lineage: LineageEvent,
}

async fn run_wire_transform(
    ctx: &TransformCtx,
    attempts: i32,
    req: WireTransform<'_>,
) -> std::result::Result<(), JobFailure> {
    // 1. Inputs register under `register_as`; DataFusion silently overwrites a
    //    same-named table, so two inputs sharing a registration name would shadow
    //    and the SQL would compute against the wrong one. Reject that up front.
    let mut seen = HashSet::new();
    for (register_as, _) in &req.inputs {
        if !seen.insert(register_as.as_str()) {
            return Err(JobFailure::abandon(format!(
                "ambiguous input table name {register_as}: two inputs would register under it"
            )));
        }
    }

    // Fresh session per job — no state leaks between transforms.
    let df_ctx = SessionContext::new();

    // 2+3. Resolve + register each input. An absent `columns` on ListFiles means the
    //      table does not exist — deterministically bad (Abandon). A live input with
    //      zero files (or a fetch streaming no batches) registers as an empty relation
    //      with the DECLARED schema so the SQL runs over an empty input.
    for (register_as, table) in &req.inputs {
        let listed = ctx
            .control
            .list_files(table.schema.clone(), table.name.clone())
            .await
            .map_err(|e| {
                JobFailure::retry(
                    ctx.worker_tuning.backoff(attempts),
                    format!("list_files: {e}"),
                )
            })?;
        let Some(columns) = listed.columns else {
            return Err(JobFailure::abandon(format!(
                "unknown input table {}.{}",
                table.schema, table.name
            )));
        };
        let batches = if listed.files.is_empty() {
            Vec::new()
        } else {
            ctx.flight
                .fetch(FlightTicket {
                    schema: table.schema.clone(),
                    name: table.name.clone(),
                    files: listed.files.iter().map(|f| f.path.clone()).collect(),
                })
                .await
                .map_err(|e| {
                    JobFailure::retry(
                        ctx.worker_tuning.backoff(attempts),
                        format!("flight fetch: {e}"),
                    )
                })?
        };
        match batches.first().map(|b| b.schema()) {
            None => {
                let schema = logical_arrow_schema(&columns)
                    .map_err(|e| JobFailure::abandon(format!("infer: {e}")))?;
                register_empty_table(&df_ctx, register_as, schema)
                    .map_err(|e| JobFailure::abandon(format!("register: {e}")))?;
            }
            Some(schema) => {
                register_batches(&df_ctx, register_as, schema, batches)
                    .map_err(|e| JobFailure::abandon(format!("register: {e}")))?;
            }
        }
    }

    // 4. Run the SQL; capture the RESULT schema (the parquet files must carry it) and
    //    the inferred output columns; typed transforms conformance-check BEFORE any
    //    rows are pulled, so a non-conforming result writes/commits nothing.
    let df = df_ctx
        .sql(req.sql)
        .await
        .map_err(|e| JobFailure::abandon(format!("sql: {e}")))?;
    let schema: Arc<arrow::datatypes::Schema> = Arc::new(df.schema().as_arrow().clone());
    let columns = infer_columns(&schema).map_err(|e| JobFailure::abandon(format!("infer: {e}")))?;
    if let Some(properties) = req.conform {
        check_conformance(&columns, properties).map_err(|violations| {
            JobFailure::abandon(format!(
                "output does not conform to the declared type ({} violation(s)): {violations:?}",
                violations.len()
            ))
        })?;
    }

    // 5. Collect + write the result as Parquet under `{schema}/{name}/{run_id}`;
    //    absolutize the paths so the serving engine can resolve them.
    let batches = df
        .collect()
        .await
        .map_err(|e| JobFailure::abandon(format!("collect: {e}")))?;
    let run_id = uuid::Uuid::new_v4().to_string();
    let dir_prefix = format!("{}/{}/{run_id}", req.output.schema, req.output.name);
    let written = write_dataset(
        ctx.write.store.clone(),
        &dir_prefix,
        schema,
        &batches,
        &ctx.write_cfg,
    )
    .await
    .map_err(|e| {
        JobFailure::retry(
            ctx.worker_tuning.backoff(attempts),
            format!("write_dataset: {e}"),
        )
    })?;
    let files = absolute_data_files(
        written,
        &ctx.write.root_url,
        &req.output.schema,
        &req.output.name,
    );

    // 6. Commit atomically over the wire: create (idempotent) + append/replace + lineage.
    let snap = ctx
        .control
        .commit_transform(
            req.output.schema.clone(),
            req.output.name.clone(),
            &columns,
            &files,
            &req.lineage,
            matches!(req.output_mode, OutputMode::Overwrite),
        )
        .await
        .map_err(|e| {
            JobFailure::retry(
                ctx.worker_tuning.backoff(attempts),
                format!("commit_transform: {e}"),
            )
        })?;
    if snap.is_none() {
        return Err(JobFailure::abandon("commit produced no snapshot id"));
    }
    Ok(())
}
