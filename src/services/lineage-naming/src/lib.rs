//! Deployment-aware naming bridge between loom's typed identities
//! (`TableRef`/`TypeName`) and OpenLineage `DatasetRef`s, and back. Built from the
//! deployment's `ObjectStoreConfig` so the physical storage location is encoded in
//! the OpenLineage namespace. Postgres-free, pure logic over config. See
//! docs/superpowers/specs/2026-07-01-dataset-naming-bridge-design.md.

use control_plane_core::{DatasetId, DatasetRef, TableRef, TypeId, TypeName};
use store_config::{ObjectStoreBackend, ObjectStoreConfig};

/// Deployment-aware bridge between loom's typed identities (`TableRef`/`TypeName`)
/// and OpenLineage `DatasetRef`s, and back. Built from the deployment's
/// `ObjectStoreConfig` so the physical storage location is encoded in the
/// OpenLineage namespace.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LineageNaming {
    /// This deployment's storage-derived OpenLineage namespace: the datasource
    /// authority only — `s3://<bucket>` or the local `file://<root>` — never the key
    /// prefix, so the namespace is stable per warehouse and the `schema.table` name
    /// stays deployment-independent.
    site_namespace: String,
}

/// The reverse-resolution outcome. Total: `External` is a first-class result, never an
/// error — a `DatasetRef` may legitimately name a dataset loom does not govern.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ResolvedDataset {
    /// Names a physical table this deployment governs.
    Table(TableRef),
    /// Names an ontology type this deployment governs.
    Type(TypeName),
    /// Names a dataset outside this deployment's governance — an external datasource,
    /// or a different loom site's warehouse. Carries the raw ref verbatim.
    External(DatasetRef),
}

impl LineageNaming {
    /// Derive the bridge from parsed deployment config. Infallible: the config is
    /// already parsed/validated, and the namespace comes from the parsed backend, so
    /// derivation cannot fail.
    pub fn from_object_store(cfg: &ObjectStoreConfig) -> LineageNaming {
        let site_namespace = match &cfg.backend {
            // S3 datasource authority is the (already-parsed, non-empty) bucket; the
            // key prefix in `warehouse_uri` is dropped so the namespace is stable per
            // warehouse and the `schema.table` name stays deployment-independent.
            ObjectStoreBackend::S3(s) => format!("s3://{}", s.bucket),
            // Local disk has no bucket authority, so the warehouse root URI itself is
            // the datasource. `warehouse_uri` was validated by `ObjectStoreConfig`.
            ObjectStoreBackend::Local => cfg.warehouse_uri.clone(),
        };
        LineageNaming { site_namespace }
    }

    /// Storage-derived ref for a governed table:
    /// `{ namespace: site_namespace, name: "<schema>.<table>" }`. Reuses `core`'s
    /// canonical `schema.table` name formatting; only the namespace is swapped for
    /// this deployment's storage datasource.
    pub fn dataset_ref(&self, table: &TableRef) -> DatasetRef {
        DatasetRef {
            namespace: self.site_namespace.clone(),
            ..DatasetId::from(table).dataset_ref()
        }
    }

    /// Storage-derived ref for a governed ontology type. Types have no physical
    /// storage of their own, so this stays on the logical type namespace
    /// (`"loom:type"`), delegating entirely to `core`.
    pub fn type_ref(&self, ty: &TypeName) -> DatasetRef {
        TypeId::from(ty).dataset_ref()
    }
}
