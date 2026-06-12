//! loom's canonical identity for a dataset it governs — a physical DuckLake table.
//! Bridges catalog `TableRef` and lineage `DatasetRef` so the two stop being joined by
//! hand-built strings. Pure logic, no I/O. See
//! docs/superpowers/specs/2026-06-12-qualified-dataset-identity-design.md.

use crate::catalog::TableRef;
use crate::lineage::DatasetRef;

/// loom's canonical logical namespace for datasets it governs. Deployment-independent:
/// a loom table's logical identity is stable regardless of which Postgres host backs the
/// catalog. External datasets (`s3://bucket`, `postgres://host`) keep their own
/// datasource-derived namespaces and are NOT loom-namespaced.
pub const LOOM_DATASET_NAMESPACE: &str = "loom";

/// loom's canonical identity for a dataset it governs — a physical DuckLake table. The
/// deployment-independent logical identity that bridges catalog `TableRef` and lineage
/// `DatasetRef`. An ontology type reaches its dataset through `ObjectType.table ->
/// DatasetId`; a type-level variant is an additive change if type-level lineage lands.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct DatasetId(TableRef);

impl DatasetId {
    /// The physical table this dataset identity refers to.
    pub fn table(&self) -> &TableRef {
        &self.0
    }

    /// The OpenLineage identity for this loom dataset: the loom namespace plus a
    /// dot-qualified `schema.name`.
    pub fn dataset_ref(&self) -> DatasetRef {
        DatasetRef {
            namespace: LOOM_DATASET_NAMESPACE.to_string(),
            name: format!("{}.{}", self.0.schema, self.0.name),
        }
    }

    /// Parse a `DatasetRef` back into a loom `DatasetId`. `None` when the ref is not
    /// loom-namespaced (it names an external dataset) or its name is not a well-formed
    /// `schema.table` (loom identifiers contain no `.`, so a multi-dot name is ambiguous
    /// and rejected rather than mis-parsed).
    pub fn from_dataset_ref(dr: &DatasetRef) -> Option<DatasetId> {
        if dr.namespace != LOOM_DATASET_NAMESPACE {
            return None;
        }
        let (schema, name) = dr.name.split_once('.')?;
        if schema.is_empty() || name.is_empty() || name.contains('.') {
            return None;
        }
        Some(DatasetId(TableRef {
            schema: schema.to_string(),
            name: name.to_string(),
        }))
    }
}

impl From<&TableRef> for DatasetId {
    fn from(table: &TableRef) -> Self {
        DatasetId(table.clone())
    }
}

/// Convenience for call sites that just want the lineage ref for a loom table.
impl From<&TableRef> for DatasetRef {
    fn from(table: &TableRef) -> Self {
        DatasetId::from(table).dataset_ref()
    }
}
