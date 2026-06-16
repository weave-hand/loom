use std::collections::HashMap;

use async_trait::async_trait;
use control_plane_core::{
    ActionDef, ActionName, ControlPlaneError, LinkDef, ObjectType, Ontology, Page, PageReq, Result,
    TableRef, TypeName,
};

use crate::MemoryControlPlane;

#[derive(Default)]
pub(crate) struct OntologyState {
    pub(crate) types: HashMap<String, ObjectType>,
    pub(crate) links: Vec<LinkDef>,
    pub(crate) actions: HashMap<String, ActionDef>,
}

#[async_trait]
impl Ontology for MemoryControlPlane {
    #[tracing::instrument(skip(self), level = "debug")]
    async fn define_type(&self, ty: ObjectType) -> Result<()> {
        self.ontology
            .lock()
            .unwrap()
            .types
            .insert(ty.name.0.clone(), ty);
        Ok(())
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn define_link(&self, link: LinkDef) -> Result<()> {
        let mut ont = self.ontology.lock().unwrap();
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

    async fn get_type(&self, name: &TypeName) -> Result<ObjectType> {
        self.ontology
            .lock()
            .unwrap()
            .types
            .get(&name.0)
            .cloned()
            .ok_or_else(|| ControlPlaneError::NotFound(name.0.clone()))
    }

    async fn list_types(&self, _page: PageReq) -> Result<Page<ObjectType>> {
        Ok(Page::from_full(
            self.ontology
                .lock()
                .unwrap()
                .types
                .values()
                .cloned()
                .collect(),
        ))
    }

    async fn links(&self, name: &TypeName, _page: PageReq) -> Result<Page<LinkDef>> {
        let ont = self.ontology.lock().unwrap();
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
        let ont = self.ontology.lock().unwrap();
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
        self.ontology
            .lock()
            .unwrap()
            .actions
            .insert(action.name.0.clone(), action);
        Ok(())
    }

    async fn get_action(&self, name: &ActionName) -> Result<ActionDef> {
        self.ontology
            .lock()
            .unwrap()
            .actions
            .get(&name.0)
            .cloned()
            .ok_or_else(|| ControlPlaneError::NotFound(name.0.clone()))
    }
}
