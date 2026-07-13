//! Shared per-dataset read governor: the readable predicate that both the
//! `/lineage` closure filter (`lineage_filter::LineageVisibility`) and the catalog
//! reads (`/datasets*`) enforce. One resolution, one soundness argument, two
//! consumers. The Table→Type fallback lives here because loom governs by type but
//! lineage emitters (and the mirror catalog) key physical datasets by table ref,
//! and `Acl::check` is deliberately exact-match.

use control_plane_core::{
    Acl, Action, ControlPlaneError, DatasetRef, Decision, Ontology, PageReq, PolicyTarget,
    SubjectId, TableRef, TypeName,
};
use lineage_naming::{LineageNaming, ResolvedDataset};

/// Borrows the ACL, the ontology, and the deployment naming bridge; owns a
/// per-request lazy `(schema, table) → backing types` map for the Table→Type
/// fallback. Construct one per request (the lazy map is not shared across requests).
pub struct DatasetVisibility<'a> {
    acl: &'a (dyn Acl + Send + Sync),
    ontology: &'a (dyn Ontology + Send + Sync),
    bridge: &'a LineageNaming,
    /// `TableRef` derives no `Ord`, so the key is its `(schema, name)` pair.
    table_types: tokio::sync::OnceCell<std::collections::BTreeMap<(String, String), Vec<TypeName>>>,
}

impl<'a> DatasetVisibility<'a> {
    /// A fresh per-request governor.
    #[must_use]
    pub fn new(
        acl: &'a (dyn Acl + Send + Sync),
        ontology: &'a (dyn Ontology + Send + Sync),
        bridge: &'a LineageNaming,
    ) -> Self {
        Self {
            acl,
            ontology,
            bridge,
            table_types: tokio::sync::OnceCell::new(),
        }
    }

    /// Classify a lineage-graph ref's readability for `subject`, resolving it through
    /// the naming bridge first. `Type` → `Acl::check(Read)`. `Table` → the Table∨Type
    /// fallback (see `is_table_readable`). `Unresolvable` (owned namespace, unparseable
    /// name) → **never readable** (denied and cut: a bridge/mapping gap can only narrow,
    /// never widen, disclosure). `External` (a genuinely foreign datasource) →
    /// default-allow: it carries no loom-ACL'd data and is a source leaf.
    pub async fn is_readable(
        &self,
        subject: &SubjectId,
        r: &DatasetRef,
    ) -> Result<bool, ControlPlaneError> {
        let table = match self.bridge.resolve(r) {
            ResolvedDataset::Table(t) => t,
            ResolvedDataset::Type(ty) => {
                return self.allows_read(subject, &PolicyTarget::Type(ty)).await;
            }
            ResolvedDataset::Unresolvable(_) => return Ok(false),
            ResolvedDataset::External(_) => return Ok(true),
        };
        self.is_table_readable(subject, &table).await
    }

    /// Readable iff the Table target allows OR a Read grant allows any ontology type
    /// *backed by* that table. The widening is sound because a type-Read grant already
    /// discloses the backing table's rows through the governed object read — seeing the
    /// table's catalog metadata / lineage node discloses strictly less. Allow-oriented:
    /// an explicit Table Deny does not veto a type Allow (consistent with the object
    /// read, which consults only the Type target).
    pub async fn is_table_readable(
        &self,
        subject: &SubjectId,
        table: &TableRef,
    ) -> Result<bool, ControlPlaneError> {
        if self
            .allows_read(subject, &PolicyTarget::Table(table.clone()))
            .await?
        {
            return Ok(true);
        }
        for ty in self.types_backed_by(table).await? {
            if self
                .allows_read(subject, &PolicyTarget::Type(ty.clone()))
                .await?
            {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// One `Acl::check(Read)` folded to a bool. Errors propagate (never disclosed as
    /// Allow).
    async fn allows_read(
        &self,
        subject: &SubjectId,
        target: &PolicyTarget,
    ) -> Result<bool, ControlPlaneError> {
        Ok(self.acl.check(subject, Action::Read, target).await? == Decision::Allow)
    }

    /// The ontology types backed by `table`, from a per-request map built lazily on the
    /// first Table-target miss with one unbounded `list_types` read (ontology metadata is
    /// deployment-sized). Requests whose Table checks all allow never pay for it.
    async fn types_backed_by(&self, table: &TableRef) -> Result<&[TypeName], ControlPlaneError> {
        let map = self
            .table_types
            .get_or_try_init(|| async {
                let types = self.ontology.list_types(PageReq::unbounded()).await?;
                let mut map: std::collections::BTreeMap<(String, String), Vec<TypeName>> =
                    std::collections::BTreeMap::new();
                for ty in types.items {
                    map.entry((ty.table.schema, ty.table.name))
                        .or_default()
                        .push(ty.name);
                }
                Ok::<_, ControlPlaneError>(map)
            })
            .await?;
        Ok(map
            .get(&(table.schema.clone(), table.name.clone()))
            .map_or(&[], Vec::as_slice))
    }
}
