//! query-api's governed Arrow Flight **export** surface. A second listener (TCP) that
//! authenticates a bearer-token caller, governs a typed-object slice exactly as the HTTP
//! read path does, and streams the engine's `RecordBatch` result straight out columnar —
//! vectors carried natively as `List<Float32>`, never flattened through `SqlValue`. The
//! Flight ticket/command carries a loom `ExportCommand`, NOT SQL; loom compiles the ACL'd
//! SQL server-side per `do_get`, so a forged/replayed ticket is still a governed request.
//! See docs/superpowers/specs/2026-06-26-governed-flight-export-design.md.

use std::pin::Pin;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_server::FlightService;
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightEndpoint, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
};
use control_plane_core::resolve_logical;
use control_plane_core::{Auth, ControlPlane, SubjectId};
use engine_wire::flight::FlightSqlClient;
use futures::{StreamExt, TryStreamExt};
use serde::{Deserialize, Serialize};
use service_runtime::{Subject, token_sha256};
use time::OffsetDateTime;
use tonic::{Request, Response, Status, Streaming};

use crate::handler::{ObjectQuery, QueryError, compile_object_read};
use crate::serving::inline_params;
use crate::sql::DataFusionDialect;

/// The governed slice to export — mirrors `GET /objects/{type}` params. JSON in the Flight
/// descriptor `cmd` (get_flight_info) and the `Ticket` bytes (do_get), mirroring
/// `FlightTicket`'s JSON convention.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExportCommand {
    /// The ontology type to export (e.g. "Chunk").
    #[serde(rename = "type")]
    pub type_name: String,
    /// Optional equality filters on allowed columns (validated server-side).
    #[serde(default)]
    pub filters: Vec<(String, String)>,
    /// Optional object-set identity values → an `In` predicate on the declared identity.
    #[serde(default)]
    pub ids: Vec<String>,
}

impl ExportCommand {
    /// JSON-encode for the descriptor `cmd` / `Ticket` bytes.
    #[expect(
        clippy::expect_used,
        reason = "ExportCommand is always serializable, mirrors FlightTicket::encode"
    )]
    pub fn encode(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("ExportCommand is always serializable")
    }
    /// Decode from descriptor `cmd` / `Ticket` bytes.
    pub fn decode(bytes: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(bytes)
    }
}

// BaseType → DataType now lives in core (BaseType::arrow_data_type) — the single
// map shared with the engine's serving schema, so get_flight_info's advertised
// schema and the do_get data schema agree by construction.

/// Build the projected Arrow schema for an export from the governed output columns, their loom
/// logical types (positionally aligned, in SELECT order), and the set of **masked** output
/// columns. Columns are nullable (the read projection does not assert non-null). A **masked**
/// column is advertised as `Utf8` — it is SELECTed as the `'***'` constant, so the engine
/// streams it as Utf8 regardless of its declared type; advertising the declared type would make
/// `get_flight_info`'s schema disagree with the `do_get` data schema. An unknown logical type is
/// an error — the ontology should never hold one.
pub fn export_arrow_schema(
    columns: &[String],
    logical_types: &[String],
    masked_columns: &[String],
) -> Result<SchemaRef, String> {
    if columns.len() != logical_types.len() {
        return Err(format!(
            "export schema: {} columns / {} types (must match)",
            columns.len(),
            logical_types.len()
        ));
    }
    let mut fields = Vec::with_capacity(columns.len());
    for (name, lt) in columns.iter().zip(logical_types) {
        let dt = if masked_columns.iter().any(|m| m == name) {
            DataType::Utf8 // masked → '***' constant streams as Utf8
        } else {
            let base = resolve_logical(lt).ok_or_else(|| format!("unknown logical type `{lt}`"))?;
            base.arrow_data_type()
        };
        fields.push(Field::new(name, dt, true));
    }
    Ok(Arc::new(Schema::new(fields)))
}

/// The governed Flight export server. Holds the auth seam, the control plane (ACL +
/// ontology), a streaming client to the engine's internal Flight-SQL plane, and the
/// per-export row cap. Spawned only when `LOOM_FLIGHT_BIND_ADDR` is set.
pub struct FlightExportService {
    auth: Arc<dyn Auth + Send + Sync>,
    cp: Arc<dyn ControlPlane>,
    engine: FlightSqlClient,
    /// Hard cap on rows per export (`LOOM_EXPORT_MAX_ROWS`). The governed SQL is compiled
    /// with `LIMIT max_rows + 1`; the outgoing stream errors once it exceeds `max_rows`, so
    /// an over-cap slice fails explicitly rather than truncating silently.
    max_rows: u32,
}

impl FlightExportService {
    pub fn new(
        auth: Arc<dyn Auth + Send + Sync>,
        cp: Arc<dyn ControlPlane>,
        engine: FlightSqlClient,
        max_rows: u32,
    ) -> Self {
        Self {
            auth,
            cp,
            engine,
            max_rows,
        }
    }
}

/// Resolve the bearer token in the gRPC `authorization` metadata to a verified subject.
/// Missing/invalid/expired → `Unauthenticated`; an auth-store fault → `Internal`.
async fn authenticate(
    auth: &(dyn Auth + Send + Sync),
    md: &tonic::metadata::MetadataMap,
) -> Result<SubjectId, Status> {
    let token = md
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer "))
        .ok_or_else(|| Status::unauthenticated("missing bearer token"))?;
    let hash = token_sha256(token);
    match auth.resolve_session(&hash, OffsetDateTime::now_utc()).await {
        Ok(Some(sid)) => Ok(sid),
        Ok(None) => Err(Status::unauthenticated("invalid or expired token")),
        Err(e) => Err(internal("flight export auth store fault", e)),
    }
}

/// Log a backend/internal fault server-side and return an opaque gRPC `Internal` status. The
/// detail (`error = %e`) is for operators only — no internal detail (SQL fragments, table/column
/// names, engine messages) reaches the external client. Mirrors the HTTP path's `internal_error`
/// (`http.rs`); this is the external Flight boundary, so scrubbing matters even more.
fn internal(context: &str, e: impl std::fmt::Display) -> Status {
    tracing::error!(error = %e, "{context}");
    Status::internal("internal error")
}

/// Map a governance error to a gRPC status. A denied type is `PermissionDenied` (before
/// existence is revealed); unknown type / bad filter is `InvalidArgument` (same validation as
/// `read_object`); backend faults are an opaque `Internal` (detail logged, never sent to the
/// client). The `InvalidArgument` messages carry only the client-supplied type/column/link name
/// (which the caller already knows), never internal SQL or schema detail.
fn map_query_err(e: QueryError) -> Status {
    match e {
        QueryError::Forbidden => Status::permission_denied("forbidden"),
        QueryError::UnknownType(t) => Status::invalid_argument(format!("unknown type: {t}")),
        QueryError::UnknownLink(l) => Status::invalid_argument(format!("unknown link: {l}")),
        QueryError::BadFilter(c) => Status::invalid_argument(format!("filter not permitted: {c}")),
        QueryError::BadFilterValue(e) => Status::invalid_argument(e.to_string()),
        QueryError::NoIdentity(t) => Status::invalid_argument(format!("type has no identity: {t}")),
        other => internal("flight export governance fault", other),
    }
}

#[tonic::async_trait]
impl FlightService for FlightExportService {
    type HandshakeStream =
        Pin<Box<dyn futures::Stream<Item = Result<HandshakeResponse, Status>> + Send>>;
    type ListFlightsStream =
        Pin<Box<dyn futures::Stream<Item = Result<FlightInfo, Status>> + Send>>;
    type DoGetStream = Pin<Box<dyn futures::Stream<Item = Result<FlightData, Status>> + Send>>;
    type DoPutStream = Pin<Box<dyn futures::Stream<Item = Result<PutResult, Status>> + Send>>;
    type DoExchangeStream = Pin<Box<dyn futures::Stream<Item = Result<FlightData, Status>> + Send>>;
    type DoActionStream =
        Pin<Box<dyn futures::Stream<Item = Result<arrow_flight::Result, Status>> + Send>>;
    type ListActionsStream =
        Pin<Box<dyn futures::Stream<Item = Result<ActionType, Status>> + Send>>;

    async fn get_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let subject = authenticate(self.auth.as_ref(), request.metadata()).await?;
        let descriptor = request.into_inner();
        let cmd = ExportCommand::decode(&descriptor.cmd)
            .map_err(|e| Status::invalid_argument(format!("bad export command: {e}")))?;
        let governed = compile_object_read(
            &ObjectQuery {
                type_name: cmd.type_name.clone(),
                filters: cmd.filters.clone(),
                ids: cmd.ids.clone(),
                or_raw: Vec::new(),
            },
            &Subject(subject),
            self.cp.ontology(),
            self.cp.acl(),
            &DataFusionDialect,
            self.max_rows.saturating_add(1),
            None,
            None,
        )
        .await
        .map_err(map_query_err)?;
        let schema = export_arrow_schema(
            &governed.columns,
            &governed.logical_types,
            &governed.masked_columns,
        )
        .map_err(|e| internal("flight export schema build", e))?;
        let endpoint = FlightEndpoint::new().with_ticket(Ticket {
            ticket: cmd.encode().into(),
        });
        let info = FlightInfo::new()
            .try_with_schema(&schema)
            .map_err(|e| internal("flight export schema encode", e))?
            .with_endpoint(endpoint)
            .with_descriptor(descriptor);
        Ok(Response::new(info))
    }

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        let subject = authenticate(self.auth.as_ref(), request.metadata()).await?;
        let ticket = request.into_inner();
        let cmd = ExportCommand::decode(&ticket.ticket)
            .map_err(|e| Status::invalid_argument(format!("bad export ticket: {e}")))?;
        // Re-derive the governed SQL for THIS authenticated subject (compile with cap+1 so an
        // over-cap slice is detectable, not silently truncated).
        let governed = compile_object_read(
            &ObjectQuery {
                type_name: cmd.type_name,
                filters: cmd.filters,
                ids: cmd.ids,
                or_raw: Vec::new(),
            },
            &Subject(subject),
            self.cp.ontology(),
            self.cp.acl(),
            &DataFusionDialect,
            self.max_rows.saturating_add(1),
            None,
            None,
        )
        .await
        .map_err(map_query_err)?;
        let sql = inline_params(&governed.sql, &governed.params);
        let batches = self
            .engine
            .execute_stream(sql)
            .await
            .map_err(|e| internal("flight export engine stream open", e))?;

        // Row cap: count rows as they stream; once cumulative rows exceed `max_rows`, emit a
        // stream error so the export fails explicitly instead of truncating. The cap message is
        // safe to surface (a fixed string + the limit). An engine/stream fault is logged
        // server-side and surfaced to the client as an OPAQUE stream error — the consumer sees a
        // failed stream (not a silent short read), but no internal detail leaks. NOTE: the cap's
        // explicit-failure guarantee holds only for `max_rows < u32::MAX` (the compile uses
        // `saturating_add(1)`); a cap of exactly `u32::MAX` cannot over-fetch the +1 sentinel.
        let max = u64::from(self.max_rows);
        let mut seen: u64 = 0;
        let capped = batches.map(move |item| match item {
            Ok(batch) => {
                // num_rows() is usize; widen losslessly to u64.
                seen += u64::try_from(batch.num_rows()).unwrap_or(u64::MAX);
                if seen > max {
                    Err(FlightError::from_external_error(Box::new(
                        std::io::Error::other(format!(
                            "export exceeded LOOM_EXPORT_MAX_ROWS ({max})"
                        )),
                    )))
                } else {
                    Ok(batch)
                }
            }
            Err(e) => {
                tracing::error!(error = %e, "flight export engine stream fault");
                Err(FlightError::from_external_error(Box::new(
                    std::io::Error::other("export stream error"),
                )))
            }
        });
        let out = FlightDataEncoderBuilder::new()
            .build(capped)
            .map_err(|e| internal("flight export encode", e));
        Ok(Response::new(Box::pin(out)))
    }

    async fn handshake(
        &self,
        _: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        Err(Status::unimplemented("handshake"))
    }
    async fn list_flights(
        &self,
        _: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented("list_flights"))
    }
    async fn poll_flight_info(
        &self,
        _: Request<FlightDescriptor>,
    ) -> Result<Response<PollInfo>, Status> {
        Err(Status::unimplemented("poll_flight_info"))
    }
    async fn get_schema(
        &self,
        _: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        Err(Status::unimplemented("get_schema"))
    }
    async fn do_put(
        &self,
        _: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        Err(Status::unimplemented("do_put"))
    }
    async fn do_exchange(
        &self,
        _: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("do_exchange"))
    }
    async fn do_action(
        &self,
        _: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        Err(Status::unimplemented("do_action"))
    }
    async fn list_actions(
        &self,
        _: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        Err(Status::unimplemented("list_actions"))
    }
}
