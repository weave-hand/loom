//! HTTP landing surface for ingest. Decodes an Arrow IPC request into schema +
//! batches, resolves the physical columns (optionally gated by a model, via
//! `materialize::resolve_columns`), and dispatches the actual write through the
//! `LandingMaterializer` port (`st.materializer.land(req)`); this layer only does
//! decode <-> HTTP mapping plus that gate/resolve step.

use std::sync::Arc;

use axum::Router;
use axum::body::Bytes;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Json, Response};
use axum::routing::post;
use control_plane_core::{
    Action, COMPACT_JOB_KIND, CompactJob, ControlPlane, ControlPlaneError, DatasetId, DatasetRef,
    Decision, LineageEvent, NewJob, PolicyTarget, RunId, TableRef, TypeName,
};
use control_plane_postgres::iceberg_landing::CdcDecl;
use serde::Deserialize;
use uuid::Uuid;

use std::borrow::Cow;

use crate::IngestError;
use crate::gate::{ColumnShape, ModelShape, validate_values};
use crate::landing::{LandRequest, LandingMaterializer};
use crate::materialize::resolve_columns;
use crate::model::{InferTypeError, infer_object_type, model_shape_from_type};
use crate::openapi::{JobAck, LandAck, ModelLandAck, ViolationsBody, WireViolation};
use service_runtime::Subject;

/// The HTTP error surface for ingest handlers. Handlers return
/// `Result<Response, ApiError>`; `IntoResponse` renders each variant, and for
/// `Internal` it logs the fault detail server-side — so fault logging is
/// structural (one place), not a per-arm chore. The client-facing bytes match
/// the former hand-rolled responses exactly (opaque `"internal error"` for 500,
/// empty 403, the message for 400, the `ViolationsBody` JSON for 422).
pub enum ApiError {
    /// A deterministic client error with a safe, client-visible message (bad IPC,
    /// bad header, unsupported column type, identity names an absent column).
    BadRequest(Cow<'static, str>),
    /// ACL deny — 403 with an empty body (no existence leak).
    Forbidden,
    /// Model-gate / conformance failures — the 422 body listing the violations.
    Violations(Vec<crate::gate::Violation>),
    /// A backend/internal fault. `detail` is logged server-side (operator-only);
    /// the response body stays the opaque `"internal error"`.
    Internal {
        context: &'static str,
        detail: String,
    },
}

impl ApiError {
    /// Build an `Internal` fault, capturing `e`'s `Display` for the server-side log.
    pub fn internal(context: &'static str, e: impl std::fmt::Display) -> Self {
        ApiError::Internal {
            context,
            detail: e.to_string(),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        match self {
            ApiError::BadRequest(msg) => (StatusCode::BAD_REQUEST, msg).into_response(),
            ApiError::Forbidden => StatusCode::FORBIDDEN.into_response(),
            ApiError::Violations(violations) => {
                let body = ViolationsBody {
                    violations: violations.iter().map(WireViolation::from).collect(),
                };
                (StatusCode::UNPROCESSABLE_ENTITY, Json(body)).into_response()
            }
            ApiError::Internal { context, detail } => {
                // The single place ingest logs a backend fault: opaque to the
                // client, diagnosable for the operator. Closes the class of
                // unlogged opaque-500 arms (iss-ingest-model-500-unlogged).
                tracing::error!(error = %detail, "{context}");
                (StatusCode::INTERNAL_SERVER_ERROR, "internal error").into_response()
            }
        }
    }
}

impl IngestError {
    /// Map a landing fault onto the HTTP surface: a conformance failure is the
    /// 422 body, an unsupported-column-type infer error is a 400 (client data),
    /// a stream-mode declaration fault (`Conflict`/`Validation` from the control
    /// plane) is a 400 with the message echoed as-is, and any other backend
    /// fault is an opaque 500 logged with `context`.
    pub fn into_api(self, context: &'static str) -> ApiError {
        match self {
            IngestError::DoesNotConform(violations) => ApiError::Violations(violations),
            IngestError::Infer(_) => ApiError::BadRequest(Cow::Borrowed("unsupported column type")),
            // Stream-mode declaration faults (a bucket-count mismatch on an
            // already-declared log table, or an attempt to convert an existing
            // batch table to a stream table) are client errors, not backend
            // faults: the message is safe to echo (it names no internal detail).
            IngestError::ControlPlane(
                ControlPlaneError::Conflict(msg) | ControlPlaneError::Validation(msg),
            ) => ApiError::BadRequest(Cow::Owned(msg)),
            other => ApiError::internal(context, other),
        }
    }
}

/// Shared, owned dependencies: the configured landing backend (Iceberg),
/// chosen at boot. Gate + schema resolution + lineage are backend-agnostic
/// and happen in the handler before dispatch.
#[derive(Clone)]
pub struct AppState {
    pub materializer: Arc<dyn LandingMaterializer>,
    pub cp: Arc<dyn ControlPlane>,
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/datasets/:schema/:table", post(land))
        .route("/models/:type", post(land_model))
        .route("/tables/:schema/:table/compact", post(compact))
        .with_state(state)
}

/// Operator action: enqueue a compaction job for `{schema}.{table}`. Returns the
/// JobId; a zero-pool worker performs the compaction asynchronously.
#[utoipa::path(
    post, path = "/tables/{schema}/{table}/compact",
    params(
        ("schema" = String, Path, description = "Iceberg schema"),
        ("table" = String, Path, description = "Table name"),
    ),
    responses(
        (status = 202, description = "Compaction job enqueued", body = JobAck),
        (status = 500, description = "Internal error"),
    ),
    security(("bearer_auth" = [])),
    tag = "tables",
)]
pub(crate) async fn compact(
    State(st): State<AppState>,
    Path((schema, table)): Path<(String, String)>,
) -> Result<Response, ApiError> {
    let payload = serde_json::to_value(CompactJob {
        schema,
        name: table,
    })
    .map_err(|e| ApiError::internal("ingest compact: serialize job payload", e))?;
    let job = NewJob {
        kind: COMPACT_JOB_KIND.to_string(),
        payload,
        run_at: None,
        priority: 0,
    };
    // Opaque on backend faults (governance-fronted service), logged server-side.
    let id = st
        .cp
        .queue()
        .enqueue(job)
        .await
        .map_err(|e| ApiError::internal("ingest compact: enqueue job", e))?;
    Ok((
        StatusCode::ACCEPTED,
        Json(serde_json::json!({ "job_id": id.0.to_string() })),
    )
        .into_response())
}

/// Parse an optional request header via `parse`: absent → `Ok(None)`, present and
/// parseable → `Ok(Some(_))`, present but unparseable → `Err(ApiError::BadRequest(err))`.
/// Collapses the per-header `match headers.get(..)` ladders into one shape.
fn parse_header<T>(
    headers: &HeaderMap,
    name: &str,
    err: &'static str,
    parse: impl Fn(&str) -> Option<T>,
) -> Result<Option<T>, ApiError> {
    match headers.get(name) {
        None => Ok(None),
        Some(v) => match v.to_str().ok().and_then(parse) {
            Some(t) => Ok(Some(t)),
            None => Err(ApiError::BadRequest(Cow::Borrowed(err))),
        },
    }
}

/// Inbound model wire DTO. The gate types are serde-free domain types, so the
/// HTTP layer owns this representation and converts.
#[derive(Deserialize)]
struct LandModel {
    columns: Vec<LandColumn>,
}

#[derive(Deserialize)]
struct LandColumn {
    name: String,
    ty: String,
    required: bool,
}

/// Query params for `POST /models/{type}`. `identity` names the column to record as the
/// inferred type's primary key (type-absent branch only; ignored when the type exists).
/// `mode=cdc&buckets=N` declares this type's table as a PK/CDC stream table on first
/// creation (requires the resolved type to have a declared identity — see `land_model`);
/// `buckets` defaults to 1 when `mode=cdc` and is otherwise ignored.
#[derive(Deserialize)]
pub(crate) struct ModelQuery {
    identity: Option<String>,
    mode: Option<String>,
    buckets: Option<i32>,
}

/// Query params for `POST /datasets/{schema}/{table}`. `mode=stream` declares the
/// table as a log table (immutable after the first successful write); `buckets`
/// is the bucket count (defaults to 1 when `mode=stream` and `buckets` is absent).
/// Both are ignored (`stream_buckets` resolves to `None`) unless `mode` is exactly
/// `"stream"`.
#[derive(Deserialize)]
pub(crate) struct StreamParams {
    mode: Option<String>,
    buckets: Option<i32>,
}

impl From<LandModel> for ModelShape {
    fn from(m: LandModel) -> Self {
        ModelShape {
            columns: m
                .columns
                .into_iter()
                .map(|c| ColumnShape {
                    name: c.name,
                    ty: c.ty,
                    required: c.required,
                    // The raw `/datasets` request carries no ontology constraints.
                    constraints: control_plane_core::PropertyConstraints::default(),
                })
                .collect(),
        }
    }
}

/// Governed model ingest. With a pre-existing type, conform the Arrow batch to it and
/// land it (slice 1). With an absent type, an authorized subject's batch *infers* an
/// `ObjectType` from the batch schema (optionally keyed by `?identity=<col>`),
/// `define_type`s it, and lands. Authorize before either branch (deny-by-default, no
/// existence leak); a denied write never reaches the store.
#[utoipa::path(
    post, path = "/models/{type}",
    params(
        ("type" = String, Path, description = "Ontology type name"),
        ("identity" = Option<String>, Query, description = "Column to record as the inferred type's identity (type-absent branch only)"),
    ),
    request_body(
        content = Vec<u8>,
        description = "Arrow IPC stream (schema + record batches)",
        content_type = "application/vnd.apache.arrow.stream",
    ),
    responses(
        (status = 200, description = "Landed as typed objects; snapshot committed", body = ModelLandAck),
        (status = 400, description = "Invalid Arrow IPC / unsupported column type / identity names an absent column"),
        (status = 403, description = "Not authorized to write the type (returned whether or not the type exists — no existence leak)"),
        (status = 422, description = "Data does not conform / an Arrow column has no loom logical type", body = ViolationsBody),
        (status = 500, description = "Internal error"),
    ),
    security(("bearer_auth" = [])),
    tag = "models",
)]
pub(crate) async fn land_model(
    State(st): State<AppState>,
    Path(type_name): Path<String>,
    Query(q): Query<ModelQuery>,
    subject: Subject,
    body: Bytes,
) -> Result<Response, ApiError> {
    let type_name = TypeName(type_name);

    // 1. Coarse ACL gate BEFORE anything is revealed: an authenticated subject without a
    //    Write grant on this type is 403 — returned whether or not the type exists (no
    //    existence leak). `require_auth` already 401s an unauthenticated caller.
    match st
        .cp
        .acl()
        .check(
            &subject.0,
            Action::Write,
            &PolicyTarget::Type(type_name.clone()),
        )
        .await
    {
        Ok(Decision::Allow) => {}
        Ok(Decision::Deny) => return Err(ApiError::Forbidden),
        Err(e) => return Err(ApiError::internal("ingest model: acl check", e)),
    }

    // 2. Decode the Arrow IPC body. Needed by both branches (inference reads the schema).
    let (schema, batches) = match datafusion_io::decode_ipc(&body) {
        Ok(sb) => sb,
        // Bad IPC is a client error (malformed request body), not a backend fault;
        // 400 without logging, exactly as before.
        Err(_) => {
            return Err(ApiError::BadRequest(Cow::Borrowed(
                "invalid arrow ipc stream",
            )));
        }
    };

    // 3. Resolve the type, or — when absent and authorized — infer it from the batch
    //    schema, create it, and re-resolve. The re-resolve is the create-or-conform race
    //    guard: `define_type` is an upsert, so concurrent first-batches resolve to one
    //    stored type; each then conforms its batch against it (the loser 422s if it
    //    differs). A granted-but-present type takes the unchanged slice-1 path.
    let otype = match st.cp.ontology().get_type(&type_name).await {
        Ok(t) => t,
        Err(ControlPlaneError::NotFound(_)) => {
            let inferred = infer_object_type(&type_name, &schema, q.identity.as_deref()).map_err(
                |e| match e {
                    InferTypeError::UnsupportedColumns(violations) => {
                        ApiError::Violations(violations)
                    }
                    InferTypeError::IdentityNotFound(col) => ApiError::BadRequest(Cow::Owned(
                        format!("identity column `{col}` is not present in the batch"),
                    )),
                },
            )?;
            st.cp.ontology().define_type(inferred).await.map_err(|e| {
                ApiError::internal("ingest model: define_type (infer-and-create)", e)
            })?;
            st.cp.ontology().get_type(&type_name).await.map_err(|e| {
                ApiError::internal("ingest model: re-resolve type after define_type", e)
            })?
        }
        Err(e) => return Err(ApiError::internal("ingest model: get_type", e)),
    };

    // `?mode=cdc&buckets=N` declares this type's table as a PK/CDC stream table on
    // first creation. Requires a declared identity (the bucket key); immutable after.
    let cdc_decl = if q.mode.as_deref() == Some("cdc") {
        let n = q.buckets.unwrap_or(1);
        if n < 1 {
            return Err(ApiError::BadRequest(Cow::Borrowed("buckets must be >= 1")));
        }
        let identity = otype
            .identity
            .clone()
            .ok_or(ApiError::BadRequest(Cow::Borrowed(
                "mode=cdc requires the type to declare an identity property",
            )))?;
        Some(CdcDecl {
            buckets: n,
            bucket_key: identity,
        })
    } else {
        None
    };

    // 4. Derive the conformance shape from the (resolved or just-created) type.
    let shape = model_shape_from_type(&otype);

    // 5. Gate + resolve the physical schema (422 + violations on mismatch).
    let columns = resolve_columns(&schema, Some(&shape))
        .map_err(|e| e.into_api("ingest model: resolve columns"))?;

    // 5b. Per-value constraint validation over the decoded batches (422 on violation),
    //     after the shape gate and before any write. The same `core` validator backs the
    //     query-api typed-insert action, so both write paths enforce identical rules.
    validate_values(&shape, &batches).map_err(ApiError::Violations)?;

    // 6. Land into the type's table with type-named lineage (rows trace to the model).
    let table = otype.table.clone();
    let type_label = type_name.0.clone();
    let file_prefix = Uuid::new_v4().to_string();
    let lineage = LineageEvent::completed(
        vec![DatasetRef::from(&type_name)],
        serde_json::json!({ "source": "http-model", "type": type_label }),
    );
    let req = LandRequest {
        table: &table,
        schema: schema.clone(),
        columns: &columns,
        batches: &batches,
        file_prefix: &file_prefix,
        lineage,
        // The `/models/{type}` path only ever declares CDC stream intent (via
        // `cdc_decl` above); the log-declare flag stays scoped to
        // `/datasets/{schema}/{table}` (Task 4).
        stream_buckets: None,
        cdc: cdc_decl,
    };

    let snap = st
        .materializer
        .land(req)
        .await
        .map_err(|e| e.into_api("ingest model: materialize"))?;
    Ok(Json(serde_json::json!({
        "snapshot_id": snap.0,
        "type": type_label,
    }))
    .into_response())
}

#[utoipa::path(
    post, path = "/datasets/{schema}/{table}",
    params(
        ("schema" = String, Path, description = "Iceberg schema"),
        ("table" = String, Path, description = "Dataset/table name"),
        ("mode" = Option<String>, Query, description = "`stream` declares this table as a log table on its first write; immutable thereafter"),
        ("buckets" = Option<i32>, Query, description = "Bucket count for `mode=stream` (defaults to 1); ignored otherwise"),
    ),
    request_body(
        content = Vec<u8>,
        description = "Arrow IPC stream (schema + record batches)",
        content_type = "application/vnd.apache.arrow.stream",
    ),
    responses(
        (status = 200, description = "Landed; snapshot committed", body = LandAck),
        (status = 400, description = "Invalid Arrow IPC / unsupported column type / bad header / invalid stream declaration"),
        (status = 422, description = "Data does not conform to model gate", body = ViolationsBody),
        (status = 500, description = "Internal error"),
    ),
    security(("bearer_auth" = [])),
    tag = "datasets",
)]
pub(crate) async fn land(
    State(st): State<AppState>,
    Path((schema_name, table_name)): Path<(String, String)>,
    headers: HeaderMap,
    Query(params): Query<StreamParams>,
    body: Bytes,
) -> Result<Response, ApiError> {
    // `?mode=stream&buckets=N` declares this table as a log table on its first
    // write (immutable thereafter); any other `mode` value (or its absence) means
    // a plain batch write, matching today's behaviour byte-for-byte.
    let stream_buckets = if params.mode.as_deref() == Some("stream") {
        let n = params.buckets.unwrap_or(1);
        if n < 1 {
            return Err(ApiError::BadRequest(Cow::Borrowed("buckets must be >= 1")));
        }
        Some(n)
    } else {
        None
    };

    // Optional model gate from X-Loom-Model (JSON) and run id from X-Loom-Run-Id.
    let gate: Option<ModelShape> =
        parse_header(&headers, "X-Loom-Model", "invalid X-Loom-Model", |s| {
            serde_json::from_str::<LandModel>(s)
                .ok()
                .map(ModelShape::from)
        })?;
    let run_id = parse_header(&headers, "X-Loom-Run-Id", "invalid X-Loom-Run-Id", |s| {
        Uuid::parse_str(s).ok().map(RunId)
    })?
    .unwrap_or_else(|| RunId(Uuid::new_v4()));

    let (schema, batches) = match datafusion_io::decode_ipc(&body) {
        Ok(sb) => sb,
        // Bad IPC is a client error (malformed request body), not a backend fault;
        // 400 without logging, exactly as before.
        Err(_) => {
            return Err(ApiError::BadRequest(Cow::Borrowed(
                "invalid arrow ipc stream",
            )));
        }
    };

    let table = TableRef {
        schema: schema_name,
        name: table_name,
    };
    let file_prefix = Uuid::new_v4().to_string();
    let lineage = LineageEvent::completed_with_run(
        run_id,
        vec![DatasetId::from(&table).dataset_ref()],
        serde_json::json!({ "source": "http-land" }),
    );

    // Gate + schema resolution: backend-agnostic, run once before dispatch.
    let columns = resolve_columns(&schema, gate.as_ref())
        .map_err(|e| e.into_api("ingest land: resolve columns"))?;

    let req = LandRequest {
        table: &table,
        schema: schema.clone(),
        columns: &columns,
        batches: &batches,
        file_prefix: &file_prefix,
        lineage,
        stream_buckets,
        // The `/datasets/{schema}/{table}` path never declares cdc intent (that
        // stays scoped to `/models/{type}?mode=cdc`).
        cdc: None,
    };

    // Opaque for backend faults: a governance-fronted service must not echo
    // internal detail (SQL, paths) to the client. `into_api` logs them server-side.
    let snap = st
        .materializer
        .land(req)
        .await
        .map_err(|e| e.into_api("ingest land: materialize"))?;
    Ok(Json(serde_json::json!({
        "snapshot_id": snap.0,
        "dataset": format!("{}.{}", table.schema, table.name),
    }))
    .into_response())
}
