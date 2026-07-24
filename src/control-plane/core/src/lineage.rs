//! The lineage concern: loom's provenance record. Every snapshot-producing run
//! emits an OpenLineage event with its input and output datasets; the `lineage`
//! schema is loom-owned (this trait reads AND writes it). Lineage stores and
//! serves provenance — it does not enforce anything.
//!
//! A `DatasetRef` is OpenLineage's own `{namespace, name}` identity, deliberately
//! decoupled from [`crate::TableRef`]/[`crate::TypeName`] so the graph can span
//! physical tables, ontology types, and external datasets alike. The graph
//! ([`Lineage::upstream`]/[`Lineage::downstream`]) is a depth-bounded transitive
//! closure over each event's input/output co-membership, capped at
//! [`LINEAGE_MAX_DEPTH`]; all three reads are cursor-paginated.

use async_trait::async_trait;
use time::OffsetDateTime;
use uuid::Uuid;

use crate::error::{ControlPlaneError, Result};
use crate::page::{Cursor, Page, PageReq};

/// An OpenLineage run identifier.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct RunId(pub Uuid);

/// OpenLineage dataset identity (`{namespace, name}` — already the OpenLineage
/// minimal shape). Decoupled from `TableRef`/`TypeName` so lineage can reference
/// physical tables, ontology types, and external datasets uniformly.
///
/// Per the OpenLineage naming spec the `namespace` is **datasource-derived** (e.g.
/// `s3://bucket`, `postgres://host:port`) and the `name` is dot-qualified
/// (`database.schema.table`). That mapping from a loom `TableRef`/`TypeName` to a
/// `DatasetRef` therefore depends on deployment context (where the data physically
/// lives), so it belongs to the consuming services, not to `core` — see
/// the GitHub issue tracker.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct DatasetRef {
    pub namespace: String,
    pub name: String,
}

/// The maximum transitive-closure depth a lineage read may request. A request
/// beyond this is rejected (`Validation`) so a caller can never trigger an
/// unbounded graph walk. A constant for now; a future env/config seam can make it
/// tunable (see the GitHub issue tracker).
pub const LINEAGE_MAX_DEPTH: u32 = 32;

/// Validate a requested closure depth. `Ok` iff `1 <= depth <= LINEAGE_MAX_DEPTH`.
/// `depth == 1` is the back-compatible one-hop read; `0` is meaningless (no hops)
/// and over-cap is an unbounded-walk guard — both are `Validation` errors.
pub fn check_depth(depth: u32) -> Result<()> {
    if depth == 0 || depth > LINEAGE_MAX_DEPTH {
        return Err(ControlPlaneError::Validation(format!(
            "lineage depth {depth} out of bounds (allowed 1..={LINEAGE_MAX_DEPTH})"
        )));
    }
    Ok(())
}

/// Encode a `(namespace, name)` keyset as an opaque page cursor for the
/// dataset-closure reads. The encoding (a JSON pair) is adapter-internal and NOT
/// part of the contract — callers round-trip it verbatim.
#[must_use]
pub fn encode_dataset_cursor(d: &DatasetRef) -> Cursor {
    // Serializing a 2-tuple of owned strings is infallible; default to an empty
    // string on the impossible error (decode of "" is a clean Validation error).
    Cursor(serde_json::to_string(&(&d.namespace, &d.name)).unwrap_or_default())
}

/// Decode a dataset cursor produced by [`encode_dataset_cursor`]. A malformed
/// cursor (not our encoding) is a `Validation` error, never a panic.
pub fn decode_dataset_cursor(c: &Cursor) -> Result<DatasetRef> {
    let (namespace, name): (String, String) = serde_json::from_str(&c.0)
        .map_err(|e| ControlPlaneError::Validation(format!("malformed lineage cursor: {e}")))?;
    Ok(DatasetRef { namespace, name })
}

/// Encode an event-sequence key (postgres `event_id` / memory insert index) as an
/// opaque cursor for `events_for` pagination.
#[must_use]
pub fn encode_event_cursor(seq: i64) -> Cursor {
    Cursor(seq.to_string())
}

/// Decode an event cursor produced by [`encode_event_cursor`].
pub fn decode_event_cursor(c: &Cursor) -> Result<i64> {
    c.0.parse::<i64>()
        .map_err(|e| ControlPlaneError::Validation(format!("malformed lineage cursor: {e}")))
}

/// Encode a `runs_for` keyset position (the max event sequence of the run's newest
/// event) as an opaque cursor. A separate name from the event cursor keeps the two
/// read paths' cursors self-documenting even though the encoding is the same i64.
#[must_use]
pub fn encode_run_cursor(seq: i64) -> Cursor {
    Cursor(seq.to_string())
}

/// Decode a cursor produced by [`encode_run_cursor`].
pub fn decode_run_cursor(c: &Cursor) -> Result<i64> {
    c.0.parse::<i64>()
        .map_err(|e| ControlPlaneError::Validation(format!("malformed lineage cursor: {e}")))
}

/// OpenLineage run-lifecycle event type. Stored; opaque to loom's own logic.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EventType {
    Start,
    Running,
    Complete,
    Abort,
    Fail,
}

impl EventType {
    /// The persisted wire token.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            EventType::Start => "start",
            EventType::Running => "running",
            EventType::Complete => "complete",
            EventType::Abort => "abort",
            EventType::Fail => "fail",
        }
    }
}

impl std::str::FromStr for EventType {
    type Err = ControlPlaneError;

    /// Parse the persisted token. Unknown tokens are a loud error (a corrupt
    /// row), never a silent default.
    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            "start" => Ok(EventType::Start),
            "running" => Ok(EventType::Running),
            "complete" => Ok(EventType::Complete),
            "abort" => Ok(EventType::Abort),
            "fail" => Ok(EventType::Fail),
            other => Err(ControlPlaneError::Validation(format!(
                "unknown event type '{other}'"
            ))),
        }
    }
}

/// Which side of a run's dataset edges a `runs_for` summary was matched on. A run
/// that both consumes and produces the queried dataset reports `Output` (the
/// producing side is the more useful "this run wrote here" signal).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RunRole {
    Input,
    Output,
}

impl RunRole {
    /// The persisted/wire token.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            RunRole::Input => "input",
            RunRole::Output => "output",
        }
    }
}

impl std::str::FromStr for RunRole {
    type Err = ControlPlaneError;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            "input" => Ok(RunRole::Input),
            "output" => Ok(RunRole::Output),
            other => Err(ControlPlaneError::Validation(format!(
                "unknown run role '{other}'"
            ))),
        }
    }
}

/// A lineage event: a typed envelope (the fields loom indexes/queries) plus the
/// full OpenLineage event stored opaquely in `payload`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LineageEvent {
    pub run_id: RunId,
    pub event_type: EventType,
    pub event_time: OffsetDateTime,
    pub inputs: Vec<DatasetRef>,
    pub outputs: Vec<DatasetRef>,
    pub payload: serde_json::Value,
}

impl LineageEvent {
    /// A completed (`EventType::Complete`) event with a freshly-minted `run_id`, the
    /// current UTC time, and no inputs — the shape emitted by the ingest land/model
    /// paths and query-api's create-from-params action. `outputs` are the datasets the
    /// run produced; `payload` is the opaque OpenLineage body.
    #[must_use]
    pub fn completed(outputs: Vec<DatasetRef>, payload: serde_json::Value) -> Self {
        Self::completed_with_run(RunId(Uuid::new_v4()), outputs, payload)
    }

    /// [`LineageEvent::completed`] against a caller-supplied `run_id` — used where the
    /// caller owns the run id (hands it to the engine to commit row + event atomically,
    /// or threads an `X-Loom-Run-Id` header through).
    #[must_use]
    pub fn completed_with_run(
        run_id: RunId,
        outputs: Vec<DatasetRef>,
        payload: serde_json::Value,
    ) -> Self {
        Self {
            run_id,
            event_type: EventType::Complete,
            event_time: OffsetDateTime::now_utc(),
            inputs: Vec::new(),
            outputs,
            payload,
        }
    }
}

/// A run that touched a queried dataset, collapsed to one row: the run id, the
/// time and type of that run's *latest* matched event, and which side
/// (input/output) the dataset sat on. Ordered newest-first by the run's max event
/// sequence (postgres `event_id` / memory emit index — monotonic with time); that
/// key is the keyset-pagination cursor. `latest_event_time` is the time of that
/// newest matched event. Powers the Catalog History tab.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RunSummary {
    pub run_id: RunId,
    pub latest_event_time: OffsetDateTime,
    pub latest_event_type: EventType,
    pub role: RunRole,
}

#[async_trait]
pub trait Lineage {
    /// Record an event (append-only). Its own transaction.
    async fn emit(&self, event: LineageEvent) -> Result<()>;
    /// All events for a run, in emit order. Empty if the run is unknown.
    /// Honors `page` (cursor + limit) — a large run is delivered in bounded pages.
    async fn events_for(&self, run: &RunId, page: PageReq) -> Result<Page<LineageEvent>>;
    /// Transitive upstream closure: every dataset reachable within `depth` hops by
    /// walking output→input edges from `dataset` (i.e. its ancestry). `depth == 1`
    /// is the one-hop read; `depth` is capped at [`LINEAGE_MAX_DEPTH`] (over-cap or
    /// `0` → `Validation`). The seed `dataset` is excluded. Result is a stable-ordered,
    /// `page`-bounded set of `DatasetRef` (no per-node depth annotation this slice).
    async fn upstream(
        &self,
        dataset: &DatasetRef,
        depth: u32,
        page: PageReq,
    ) -> Result<Page<DatasetRef>>;
    /// Transitive downstream closure: every dataset reachable within `depth` hops by
    /// walking input→output edges from `dataset` (i.e. its descendancy). Same depth
    /// cap, seed exclusion, ordering, and pagination as [`Lineage::upstream`].
    async fn downstream(
        &self,
        dataset: &DatasetRef,
        depth: u32,
        page: PageReq,
    ) -> Result<Page<DatasetRef>>;
    /// The distinct runs whose events reference `dataset` (as input or output),
    /// newest-first by the run's max event sequence. Each run is collapsed to one
    /// [`RunSummary`] carrying its latest matched event's time/type and the matched
    /// role. Cursor-paginated via `page`. Empty if the dataset appears in no event.
    async fn runs_for(&self, dataset: &DatasetRef, page: PageReq) -> Result<Page<RunSummary>>;
}
