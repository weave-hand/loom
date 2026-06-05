//! The lineage concern: loom's provenance record. Every snapshot-producing run
//! emits an OpenLineage event with its input and output datasets; the `lineage`
//! schema is loom-owned (this trait reads AND writes it). Lineage stores and
//! serves provenance — it does not enforce anything.
//!
//! A `DatasetRef` is OpenLineage's own `{namespace, name}` identity, deliberately
//! decoupled from [`crate::TableRef`]/[`crate::TypeName`] so the graph can span
//! physical tables, ontology types, and external datasets alike. The graph
//! ([`Lineage::upstream`]/[`Lineage::downstream`]) is one hop, computed from each
//! event's own input/output co-membership.

use async_trait::async_trait;
use time::OffsetDateTime;
use uuid::Uuid;

use crate::error::Result;

/// An OpenLineage run identifier.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct RunId(pub Uuid);

/// OpenLineage dataset identity. Decoupled from `TableRef`/`TypeName` so lineage
/// can reference physical tables, ontology types, and external datasets uniformly.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct DatasetRef {
    pub namespace: String,
    pub name: String,
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
    async fn events_for(&self, run: &RunId) -> Result<Vec<LineageEvent>>;
    /// One hop: datasets that fed directly into a run that produced `dataset`
    /// (order unspecified). Empty if `dataset` is unknown.
    async fn upstream(&self, dataset: &DatasetRef) -> Result<Vec<DatasetRef>>;
    /// One hop: datasets produced directly by a run that consumed `dataset`
    /// (order unspecified). Empty if `dataset` is unknown.
    async fn downstream(&self, dataset: &DatasetRef) -> Result<Vec<DatasetRef>>;
}
