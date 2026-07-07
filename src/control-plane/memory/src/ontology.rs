use std::collections::HashMap;

use async_trait::async_trait;
use control_plane_core::{
    ActionDef, ActionName, ControlPlaneError, LinkDef, ObjectType, Ontology, Page, PageReq, Result,
    TableRef, TransformBody, TransformName, TriggerNode, TypeName, VectorIndexDef,
    validate_no_multi_def_trigger_cycle,
};

use crate::MemoryControlPlane;

#[derive(Default)]
pub(crate) struct OntologyState {
    pub(crate) types: HashMap<String, ObjectType>,
    pub(crate) links: Vec<LinkDef>,
    pub(crate) actions: HashMap<String, ActionDef>,
    pub(crate) vector_indexes: HashMap<(String, String), VectorIndexDef>,
}

#[async_trait]
impl Ontology for MemoryControlPlane {
    #[tracing::instrument(skip(self), level = "debug")]
    async fn define_type(&self, ty: ObjectType) -> Result<()> {
        // Reject malformed constraint declarations at define time — same gate as the
        // postgres adapter, so both reject identically (the testkit contract pins this).
        control_plane_core::validate_constraints(&ty.properties)?;
        // Emit the type↔table binding edge iff the type is new or its backing table
        // changed (source guard: an unchanged re-define emits nothing). Lock order is
        // ontology-then-lineage; no other memory path holds both, so nesting is safe.
        let mut ont = self.ontology.lock();
        let changed = ont
            .types
            .get(&ty.name.0)
            .is_none_or(|prev| prev.table != ty.table);
        if changed {
            // Validate the data-triggered DAG against the WOULD-BE binding before
            // committing the rebind, so a cycle-creating rebind leaves state untouched.
            let mut type_tables: std::collections::HashMap<String, TableRef> = ont
                .types
                .iter()
                .map(|(n, t)| (n.clone(), t.table.clone()))
                .collect();
            type_tables.insert(ty.name.0.clone(), ty.table.clone()); // the new binding wins
            let dt: Vec<(TransformName, TransformBody)> = {
                let t = self.transforms.lock();
                t.defs
                    .values()
                    .filter(|d| d.on_input_commit)
                    .map(|d| (d.name.clone(), d.body.clone()))
                    .collect()
            }; // transforms lock dropped here (ontology still held — ontology-first order)
            let nodes: Vec<TriggerNode> = dt
                .iter()
                .map(|(n, b)| TriggerNode::resolve(n, b, &type_tables))
                .collect();
            validate_no_multi_def_trigger_cycle(&nodes)?;
        }
        let mut targets: std::collections::HashMap<String, ObjectType> =
            std::collections::HashMap::new();
        for l in ont.links.iter().filter(|l| l.from == ty.name) {
            let target = if l.to == ty.name {
                ty.clone()
            } else if let Some(t) = ont.types.get(&l.to.0) {
                t.clone()
            } else {
                continue;
            };
            targets.insert(l.name.clone(), target);
        }
        control_plane_core::validate_derived_columns(&ty.derived, |ln| targets.get(ln))?;
        let event = changed.then(|| control_plane_core::type_table_binding_event(&ty));
        ont.types.insert(ty.name.0.clone(), ty);
        if let Some(event) = event {
            self.lineage.lock().events.push(event);
        }
        Ok(())
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn define_link(&self, link: LinkDef) -> Result<()> {
        let mut ont = self.ontology.lock();
        for endpoint in [&link.from, &link.to] {
            if !ont.types.contains_key(&endpoint.0) {
                return Err(ControlPlaneError::NotFound(format!("type {}", endpoint.0)));
            }
        }
        ont.links
            .retain(|l| !(l.name == link.name && l.from == link.from));
        ont.links.push(link);
        Ok(())
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn delete_link(&self, from: &TypeName, name: &str) -> Result<()> {
        let mut ont = self.ontology.lock(); // ONE lock: read refs + retain atomically (no TOCTOU)
        let refs: Vec<String> = ont
            .types
            .get(&from.0)
            .map(|t| {
                t.derived
                    .iter()
                    .filter(|d| d.link == name)
                    .map(|d| d.name.clone())
                    .collect()
            })
            .unwrap_or_default();
        if !refs.is_empty() {
            return Err(ControlPlaneError::Conflict(format!(
                "link `{name}` is referenced by derived properties: {}",
                refs.join(", ")
            )));
        }
        ont.links.retain(|l| !(l.from == *from && l.name == name));
        Ok(())
    }

    async fn derived_properties_referencing(
        &self,
        from: &TypeName,
        name: &str,
    ) -> Result<Vec<String>> {
        let ont = self.ontology.lock();
        let names = ont
            .types
            .get(&from.0)
            .map(|t| {
                t.derived
                    .iter()
                    .filter(|d| d.link == name)
                    .map(|d| d.name.clone())
                    .collect()
            })
            .unwrap_or_default();
        Ok(names)
    }

    async fn get_type(&self, name: &TypeName) -> Result<ObjectType> {
        self.ontology
            .lock()
            .types
            .get(&name.0)
            .cloned()
            .ok_or_else(|| ControlPlaneError::NotFound(name.0.clone()))
    }

    async fn list_types(&self, _page: PageReq) -> Result<Page<ObjectType>> {
        Ok(Page::from_full(
            self.ontology.lock().types.values().cloned().collect(),
        ))
    }

    async fn links(&self, name: &TypeName, _page: PageReq) -> Result<Page<LinkDef>> {
        let ont = self.ontology.lock();
        if !ont.types.contains_key(&name.0) {
            return Err(ControlPlaneError::NotFound(name.0.clone()));
        }
        Ok(Page::from_full(
            ont.links
                .iter()
                .filter(|l| l.from == *name)
                .cloned()
                .collect(),
        ))
    }

    async fn links_to(&self, name: &TypeName, _page: PageReq) -> Result<Page<LinkDef>> {
        let ont = self.ontology.lock();
        if !ont.types.contains_key(&name.0) {
            return Err(ControlPlaneError::NotFound(name.0.clone()));
        }
        Ok(Page::from_full(
            ont.links
                .iter()
                .filter(|l| l.to == *name)
                .cloned()
                .collect(),
        ))
    }

    async fn resolve(&self, name: &TypeName) -> Result<TableRef> {
        Ok(self.get_type(name).await?.table)
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn define_action(&self, action: ActionDef) -> Result<()> {
        let mut ont = self.ontology.lock();
        // Single-step semantics: validate the sole step's target exists, then store the
        // whole action by clone (the map is step-shape-agnostic).
        let step = action.steps.first().ok_or_else(|| {
            ControlPlaneError::Validation(format!("action `{}` has no steps", action.name.0))
        })?;
        if !ont.types.contains_key(&step.target.0) {
            return Err(ControlPlaneError::Validation(format!(
                "action `{}` references unknown target type `{}`",
                action.name.0, step.target.0
            )));
        }
        ont.actions.insert(action.name.0.clone(), action);
        Ok(())
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn delete_action(&self, name: &ActionName) -> Result<()> {
        self.ontology.lock().actions.remove(&name.0);
        Ok(())
    }

    async fn get_action(&self, name: &ActionName) -> Result<ActionDef> {
        self.ontology
            .lock()
            .actions
            .get(&name.0)
            .cloned()
            .ok_or_else(|| ControlPlaneError::NotFound(name.0.clone()))
    }

    async fn list_actions(&self, _page: PageReq) -> Result<Page<ActionDef>> {
        let mut out: Vec<ActionDef> = self.ontology.lock().actions.values().cloned().collect();
        out.sort_by(|a, b| a.name.0.cmp(&b.name.0));
        Ok(Page::from_full(out))
    }

    async fn define_vector_index(&self, def: VectorIndexDef) -> Result<()> {
        let mut ont = self.ontology.lock();
        let ty = ont
            .types
            .get(&def.type_name.0)
            .ok_or_else(|| ControlPlaneError::NotFound(def.type_name.0.clone()))?;
        match ty.properties.iter().find(|p| p.name == def.property) {
            Some(p) if p.ty.starts_with("vector(") => {}
            Some(_) => {
                return Err(ControlPlaneError::Validation(format!(
                    "property `{}` on type `{}` is not a vector type",
                    def.property, def.type_name.0
                )));
            }
            None => {
                return Err(ControlPlaneError::Validation(format!(
                    "type `{}` has no property `{}`",
                    def.type_name.0, def.property
                )));
            }
        }
        ont.vector_indexes
            .insert((def.type_name.0.clone(), def.name.clone()), def);
        Ok(())
    }

    async fn get_vector_index(
        &self,
        type_name: &TypeName,
        name: &str,
    ) -> Result<Option<VectorIndexDef>> {
        Ok(self
            .ontology
            .lock()
            .vector_indexes
            .get(&(type_name.0.clone(), name.to_string()))
            .cloned())
    }

    async fn vector_indexes_for(&self, type_name: &TypeName) -> Result<Vec<VectorIndexDef>> {
        Ok(self
            .ontology
            .lock()
            .vector_indexes
            .values()
            .filter(|d| d.type_name == *type_name)
            .cloned()
            .collect())
    }
}
