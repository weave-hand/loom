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
/// `docs/FUTURE.md`.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct DatasetRef {
    pub namespace: String,
    pub name: String,
}

/// The maximum transitive-closure depth a lineage read may request. A request
/// beyond this is rejected (`Validation`) so a caller can never trigger an
/// unbounded graph walk. A constant for now; a future env/config seam can make it
/// tunable (see `docs/FUTURE.md`).
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

/// OpenLineage run-lifecycle event type. Stored; opaque to loom's own logic.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EventType {
    Start,
    Running,
    Complete,
    Abort,
    Fail,
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
}
