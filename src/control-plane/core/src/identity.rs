//! loom's canonical identity for a dataset it governs — a physical DuckLake table.
//! Bridges catalog `TableRef` and lineage `DatasetRef` so the two stop being joined by
//! hand-built strings. Pure logic, no I/O. See
//! docs/superpowers/specs/2026-06-12-qualified-dataset-identity-design.md.

use crate::catalog::TableRef;
use crate::lineage::DatasetRef;
use crate::ontology::TypeName;

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

/// loom's canonical logical namespace for ontology *types* it governs. Distinct from
/// `LOOM_DATASET_NAMESPACE` so a type "Customer" and a table "x.Customer" never collide.
pub const LOOM_TYPE_NAMESPACE: &str = "loom:type";

/// loom's canonical lineage identity for an ontology type. Parallel to [`DatasetId`]
/// (which identifies a physical table); a typed transform's provenance nodes are these.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct TypeId(TypeName);

impl TypeId {
    /// The ontology type this identity refers to.
    pub fn type_name(&self) -> &TypeName {
        &self.0
    }

    /// The OpenLineage identity for this type: the loom-type namespace plus the bare
    /// type name (type names are single identifiers, not schema-qualified).
    pub fn dataset_ref(&self) -> DatasetRef {
        DatasetRef {
            namespace: LOOM_TYPE_NAMESPACE.to_string(),
            name: self.0.0.clone(),
        }
    }

    /// Parse a `DatasetRef` back into a `TypeId`. `None` when the ref is not
    /// loom-type-namespaced or its name is empty.
    pub fn from_dataset_ref(dr: &DatasetRef) -> Option<TypeId> {
        if dr.namespace != LOOM_TYPE_NAMESPACE || dr.name.is_empty() {
            return None;
        }
        Some(TypeId(TypeName(dr.name.clone())))
    }
}

impl From<&TypeName> for TypeId {
    fn from(name: &TypeName) -> Self {
        TypeId(name.clone())
    }
}

/// Convenience for call sites that just want the lineage ref for an ontology type.
impl From<&TypeName> for DatasetRef {
    fn from(name: &TypeName) -> Self {
        TypeId::from(name).dataset_ref()
    }
}
