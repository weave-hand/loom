//! Pure DTOs + parameter/serialization helpers for the governed lineage read
//! endpoints (the `/lineage` HTTP surface). The thin axum handlers live in
//! `http.rs`; this module holds the pieces that are unit-testable without a router
//! or Postgres: the response shapes, the `after`/`limit` -> `PageReq` parse, and
//! the `Page<T>` -> JSON serialization.

use control_plane_core::{
    Cursor, DatasetRef, EventType, LineageEvent, Page, PageReq, RunRole, RunSummary,
};
use utoipa::ToSchema;

/// One provenance node: an OpenLineage `{namespace, name}` dataset identity.
#[derive(serde::Serialize, ToSchema)]
pub struct DatasetNode {
    pub namespace: String,
    pub name: String,
}

impl From<DatasetRef> for DatasetNode {
    fn from(d: DatasetRef) -> Self {
        Self {
            namespace: d.namespace,
            name: d.name,
        }
    }
}

/// Response for the upstream/downstream closure reads: a flat set of datasets plus
/// the opaque next-page cursor (`null` on the last page). No per-node depth — the
/// capability returns a set, matching its contract.
#[derive(serde::Serialize, ToSchema)]
pub struct DatasetClosureResponse {
    pub datasets: Vec<DatasetNode>,
    pub next_cursor: Option<String>,
}

/// Serialization shape for one lineage event. `event_time` is a pre-formatted
/// RFC3339 string (the `time` crate's serde-well-known feature is not enabled);
/// `payload` is the opaque OpenLineage event carried verbatim.
#[derive(serde::Serialize, ToSchema)]
pub struct LineageEventView {
    pub run_id: String,
    pub event_type: String,
    pub event_time: String,
    pub inputs: Vec<DatasetNode>,
    pub outputs: Vec<DatasetNode>,
    // `serde_json::Value` derives `ToSchema` directly (see openapi.rs) — no
    // `#[schema(value_type = ...)]` override needed.
    pub payload: serde_json::Value,
}

/// Response for the run-events read.
#[derive(serde::Serialize, ToSchema)]
pub struct RunEventsResponse {
    pub events: Vec<LineageEventView>,
    pub next_cursor: Option<String>,
}

/// Stable string tag for an event type (matches the adapter's own encoding).
#[must_use]
pub fn event_type_str(t: EventType) -> &'static str {
    match t {
        EventType::Start => "start",
        EventType::Running => "running",
        EventType::Complete => "complete",
        EventType::Abort => "abort",
        EventType::Fail => "fail",
    }
}

/// Build a `PageReq` from the raw `after` (opaque cursor) + `limit` query params.
/// A non-numeric `limit` is a caller error (`Err(message)` -> 400 at the handler).
/// An absent limit is unbounded (`None`); an absent cursor starts from the beginning.
/// The cursor is opaque — round-tripped verbatim, never decoded here.
pub fn parse_lineage_page(after: Option<String>, limit: Option<String>) -> Result<PageReq, String> {
    let limit = match limit {
        Some(s) => Some(
            s.parse::<u32>()
                // Carry the parse error — a bare `.map_err(|_| ...)` trips the enforced
                // `clippy::map_err_ignore` (restriction group) on production code.
                .map_err(|e| format!("limit must be a non-negative integer: {e}"))?,
        ),
        None => None,
    };
    Ok(PageReq {
        after: after.map(Cursor),
        limit,
    })
}

/// Serialize a dataset-closure page into its response DTO.
#[must_use]
pub fn dataset_closure_body(page: Page<DatasetRef>) -> DatasetClosureResponse {
    let next_cursor = page.next.map(|c| c.0);
    DatasetClosureResponse {
        datasets: page.items.into_iter().map(DatasetNode::from).collect(),
        next_cursor,
    }
}

/// Serialize one lineage event into its view DTO (RFC3339 time, string tags).
#[must_use]
pub fn lineage_event_view(e: LineageEvent) -> LineageEventView {
    let event_time = e
        .event_time
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_default();
    LineageEventView {
        run_id: e.run_id.0.to_string(),
        event_type: event_type_str(e.event_type).to_string(),
        event_time,
        inputs: e.inputs.into_iter().map(DatasetNode::from).collect(),
        outputs: e.outputs.into_iter().map(DatasetNode::from).collect(),
        payload: e.payload,
    }
}

/// Serialize a run-events page into its response DTO.
#[must_use]
pub fn run_events_body(page: Page<LineageEvent>) -> RunEventsResponse {
    let next_cursor = page.next.map(|c| c.0);
    RunEventsResponse {
        events: page.items.into_iter().map(lineage_event_view).collect(),
        next_cursor,
    }
}

/// Stable string tag for a run role (matches the adapter's own encoding).
#[must_use]
pub fn run_role_str(r: RunRole) -> &'static str {
    match r {
        RunRole::Input => "input",
        RunRole::Output => "output",
    }
}

/// One run that touched a dataset: the run id, its latest event's RFC3339 time and
/// string-tagged type, and the matched role (`input`/`output`).
#[derive(serde::Serialize, ToSchema)]
pub struct DatasetRunView {
    pub run_id: String,
    pub latest_event_time: String,
    pub latest_event_type: String,
    pub role: String,
}

/// Response for the per-dataset runs read.
#[derive(serde::Serialize, ToSchema)]
pub struct DatasetRunsResponse {
    pub runs: Vec<DatasetRunView>,
    pub next_cursor: Option<String>,
}

/// Serialize one run summary into its view DTO (RFC3339 time, string tags).
#[must_use]
pub fn dataset_run_view(s: RunSummary) -> DatasetRunView {
    let latest_event_time = s
        .latest_event_time
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_default();
    DatasetRunView {
        run_id: s.run_id.0.to_string(),
        latest_event_time,
        latest_event_type: event_type_str(s.latest_event_type).to_string(),
        role: run_role_str(s.role).to_string(),
    }
}

/// Serialize a per-dataset runs page into its response DTO.
#[must_use]
pub fn dataset_runs_body(page: Page<RunSummary>) -> DatasetRunsResponse {
    let next_cursor = page.next.map(|c| c.0);
    DatasetRunsResponse {
        runs: page.items.into_iter().map(dataset_run_view).collect(),
        next_cursor,
    }
}
