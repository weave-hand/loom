//! query-api's external Flight SQL wire: a raw TCP `FlightService` that accepts a
//! bearer-authenticated caller's ARBITRARY SQL over the standard Arrow Flight SQL
//! protobuf commands (`CommandStatementQuery` / `TicketStatementQuery`), resolves
//! that caller's governed catalog SERVER-SIDE from the authenticated subject, and
//! forwards the query to the engine's internal Flight-SQL plane under that catalog.
//! Governance is by construction, not by parsing the client's SQL: the catalog is
//! never accepted from the wire — see the load-bearing comment on `do_get` below.
//! Mirrors the governed Flight export's (`flight_export.rs`) raw-trait skeleton,
//! per-verb auth, stream relay, row cap, and error scrubbing; the divergence here
//! is that the SQL is the caller's own, not a loom-compiled export query, so a
//! plan/validation fault is a legitimate client error (`invalid_argument`), not an
//! internal fault.

use std::pin::Pin;
use std::sync::Arc;

use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_server::FlightService;
use arrow_flight::sql::{Any, CommandStatementQuery, ProstMessageExt, TicketStatementQuery};
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightEndpoint, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
};
use control_plane_core::{Auth, ControlPlane};
use engine_wire::flight::FlightSqlClient;
use futures::{StreamExt, TryStreamExt};
use prost::Message;
use tonic::{Request, Response, Status, Streaming};

use crate::governed::resolve_governed_catalog;
use crate::handler::QueryError;

/// The external Flight SQL wire server. Holds the auth seam, the control plane
/// (ACL + ontology, used only to resolve the caller's governed catalog per call —
/// never to parse/rewrite their SQL), a streaming client to the engine's internal
/// governed-SQL plane, and the per-query row cap. Spawned only when
/// `LOOM_SQL_WIRE_BIND_ADDR` is set.
pub struct FlightSqlWireService {
    auth: Arc<dyn Auth + Send + Sync>,
    cp: Arc<dyn ControlPlane>,
    engine: FlightSqlClient,
    /// Hard cap on rows per query (`LOOM_SQL_WIRE_MAX_ROWS`). Unlike the export's
    /// compiled-SQL cap (which bakes `LIMIT max_rows + 1` into loom-generated SQL
    /// so an over-cap result is detectable before it's fully streamed), this wire
    /// forwards the client's own arbitrary SQL verbatim — it is never rewritten —
    /// so there is no `+1` sentinel to request. The cap here is pure stream-side
    /// counting: once cumulative rows exceed `max_rows`, the outgoing stream errors
    /// explicitly rather than truncating silently.
    max_rows: u32,
}

impl FlightSqlWireService {
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

/// Log a backend/internal fault server-side and return an opaque gRPC `Internal`
/// status. The detail (`error = %e`) is for operators only — no internal detail
/// reaches the external client. Mirrors `flight_export.rs`'s `internal` helper;
/// duplicated locally rather than shared because it's a two-line leaf helper and
/// the two modules must not couple their error-scrubbing surfaces.
fn internal(context: &str, e: impl std::fmt::Display) -> Status {
    tracing::error!(error = %e, "{context}");
    Status::internal("internal error")
}

/// Map a governance error (from resolving the caller's governed catalog) to a
/// gRPC status. Local clone of `flight_export::map_query_err` — same class-
/// preserving scrubbing: a denied type is `PermissionDenied`, unknown
/// type/link/filter is `InvalidArgument` (client already knows the name it gave),
/// backend faults are an opaque `Internal` (detail logged, never sent). In
/// practice `resolve_governed_catalog` only ever fails with the `ControlPlane`
/// backend variant (it has no per-type existence checks to deny/miss on), but the
/// full match is kept so this stays a faithful, drift-proof clone of the export's
/// mapping rather than a narrower one that silently diverges if either evolves.
fn map_query_err(e: QueryError) -> Status {
    match e {
        QueryError::Forbidden => Status::permission_denied("forbidden"),
        QueryError::UnknownType(t) => Status::invalid_argument(format!("unknown type: {t}")),
        QueryError::UnknownLink(l) => Status::invalid_argument(format!("unknown link: {l}")),
        QueryError::BadFilter(c) => Status::invalid_argument(format!("filter not permitted: {c}")),
        QueryError::BadFilterValue(e) => Status::invalid_argument(e.to_string()),
        QueryError::NoIdentity(t) => Status::invalid_argument(format!("type has no identity: {t}")),
        other => internal("sql wire governance fault", other),
    }
}

/// Map the engine's governed-SQL plane error to a gRPC status. Unlike the export
/// (whose SQL loom compiled itself, so any failure there is an internal bug),
/// this wire forwards the CLIENT'S OWN SQL, so a `Plan` fault (bad syntax,
/// unknown/unlisted table — the engine's closed-world catalog makes an
/// ungoverned table indistinguishable from a nonexistent one) is a legitimate
/// client error: `InvalidArgument` carrying the engine's plan message, which is
/// safe to echo because it is entirely in the client's own SQL vocabulary. A
/// `ResourceExhausted` fault (valid SQL, over its memory/wall-clock budget) is
/// also safe to surface directly — the caller should narrow it and retry.
/// Everything else (a real backend/execution fault) stays an opaque `Internal`.
fn map_engine_err(e: engine_wire::client::GovernedSqlError) -> Status {
    use engine_wire::client::GovernedSqlError as E;
    match e {
        E::Plan(msg) => Status::invalid_argument(msg),
        // Valid SQL, over budget — surface the gRPC status directly so the external
        // client can tell "narrow it and retry" from "the server is broken".
        E::ResourceExhausted(msg) => Status::resource_exhausted(msg),
        other => internal("sql wire engine stream open", other),
    }
}

#[tonic::async_trait]
impl FlightService for FlightSqlWireService {
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
        crate::flight_auth::authenticate(self.auth.as_ref(), request.metadata()).await?;
        let descriptor = request.into_inner();
        // Mirrors engine/src/flight.rs's own get_flight_info dance verbatim (this
        // wire's client-facing surface is the same protocol the engine's internal
        // plane already speaks): decode the descriptor `cmd` as a protobuf `Any`,
        // unpack it as `CommandStatementQuery` only.
        let any = Any::decode(&descriptor.cmd[..])
            .map_err(|e| Status::invalid_argument(format!("bad flight-sql command: {e}")))?;
        let cmd = any
            .unpack::<CommandStatementQuery>()
            .map_err(|e| Status::invalid_argument(format!("bad CommandStatementQuery: {e}")))?
            .ok_or_else(|| {
                Status::unimplemented("only CommandStatementQuery is supported on this wire")
            })?;
        // No schema is attached to the FlightInfo: the query is arbitrary client SQL,
        // never planned/compiled here ahead of do_get, so there is no schema yet to
        // advertise. The client reads the result schema from the do_get stream's
        // first (schema) message, same as any Flight SQL client does against a plan
        // whose shape isn't known until execution.
        let ticket = TicketStatementQuery {
            statement_handle: cmd.query.into_bytes().into(),
        };
        let endpoint = FlightEndpoint::new().with_ticket(Ticket {
            ticket: ticket.as_any().encode_to_vec().into(),
        });
        let info = FlightInfo::new()
            .with_endpoint(endpoint)
            .with_descriptor(descriptor);
        Ok(Response::new(info))
    }

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        let subject =
            crate::flight_auth::authenticate(self.auth.as_ref(), request.metadata()).await?;
        let ticket = request.into_inner();

        // SECURITY (load-bearing): this decodes ONLY a protobuf Any-packed
        // `TicketStatementQuery` — the standard Flight SQL statement-handle ticket,
        // which carries nothing but the client's own SQL string. It must NEVER
        // decode a loom-native ticket such as `engine_wire::flight::GovernedStatementQuery`
        // (the internal engine-wire ticket, which embeds its own `GovernedCatalog`):
        // accepting one here would let an external caller hand us a catalog of ITS
        // OWN choosing — a total governance bypass. Any non-`TicketStatementQuery`
        // payload (including every loom-native JSON ticket, none of which are valid
        // protobuf `Any` to begin with) is rejected outright. The catalog for this
        // call is ALWAYS resolved below, server-side, from the bearer-authenticated
        // subject — the client never supplies or influences it.
        let any = Any::decode(&ticket.ticket[..])
            .map_err(|e| Status::invalid_argument(format!("bad flight-sql ticket: {e}")))?;
        let stmt = any
            .unpack::<TicketStatementQuery>()
            .map_err(|e| Status::invalid_argument(format!("bad flight-sql ticket: {e}")))?
            .ok_or_else(|| Status::invalid_argument("bad flight-sql ticket"))?;
        let sql = String::from_utf8(stmt.statement_handle.to_vec())
            .map_err(|e| Status::invalid_argument(format!("bad flight-sql ticket: {e}")))?;

        let catalog = resolve_governed_catalog(self.cp.ontology(), self.cp.acl(), &subject)
            .await
            .map_err(map_query_err)?;

        let batches = self
            .engine
            .execute_governed_stream(sql, catalog)
            .await
            .map_err(map_engine_err)?;

        // Row cap: count rows as they stream; once cumulative rows exceed `max_rows`,
        // emit a stream error so the query fails explicitly instead of truncating.
        // The cap message is safe to surface (a fixed string + the limit). An
        // engine/stream fault is logged server-side and surfaced to the client as an
        // OPAQUE stream error — the consumer sees a failed stream (not a silent short
        // read), but no internal detail leaks. Copied from flight_export.rs's cap
        // block (~:267-291); diverges only in naming the `LOOM_SQL_WIRE_MAX_ROWS`
        // knob and in having no `+1` sentinel to rely on (see the `max_rows` field
        // doc) — the counting itself is identical.
        let max = u64::from(self.max_rows);
        let mut seen: u64 = 0;
        let capped = batches.map(move |item| match item {
            Ok(batch) => {
                // num_rows() is usize; widen losslessly to u64.
                seen += u64::try_from(batch.num_rows()).unwrap_or(u64::MAX);
                if seen > max {
                    Err(FlightError::from_external_error(Box::new(
                        std::io::Error::other(format!(
                            "sql wire exceeded LOOM_SQL_WIRE_MAX_ROWS ({max})"
                        )),
                    )))
                } else {
                    Ok(batch)
                }
            }
            Err(engine_wire::client::GovernedSqlError::ResourceExhausted(m)) => {
                // Safe to surface: the message names the budget that was exceeded,
                // never data. Carried as a `Tonic` item so the encoder's tail keeps
                // the `resource_exhausted` code instead of collapsing it.
                Err(FlightError::Tonic(Box::new(Status::resource_exhausted(m))))
            }
            Err(e) => {
                tracing::error!(error = %e, "sql wire engine stream fault");
                Err(FlightError::from_external_error(Box::new(
                    std::io::Error::other("sql wire stream error"),
                )))
            }
        });
        let out = FlightDataEncoderBuilder::new()
            .build(capped)
            .map_err(|e| match e {
                // A status this handler formed itself (the budget breach) keeps its
                // code; everything else stays an opaque, logged `internal`.
                FlightError::Tonic(s) => *s,
                other => internal("sql wire encode", other),
            });
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
