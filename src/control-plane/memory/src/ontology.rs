use std::collections::HashMap;

use async_trait::async_trait;
use control_plane_core::{
    ControlPlaneError, LinkDef, ObjectType, Ontology, Result, TableRef, TypeName,
};

use crate::MemoryControlPlane;

#[derive(Default)]
pub(crate) struct OntologyState {
    pub(crate) types: HashMap<String, ObjectType>,
    pub(crate) links: Vec<LinkDef>,
}

#[async_trait]
impl Ontology for MemoryControlPlane {
    async fn define_type(&self, ty: ObjectType) -> Result<()> {
        self.ontology
            .lock()
            .unwrap()
            .types
            .insert(ty.name.0.clone(), ty);
        Ok(())
    }

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

    async fn list_types(&self) -> Result<Vec<ObjectType>> {
        Ok(self
            .ontology
            .lock()
            .unwrap()
            .types
            .values()
            .cloned()
            .collect())
    }

    async fn links(&self, name: &TypeName) -> Result<Vec<LinkDef>> {
        let ont = self.ontology.lock().unwrap();
        if !ont.types.contains_key(&name.0) {
            return Err(ControlPlaneError::NotFound(name.0.clone()));
        }
        Ok(ont
            .links
            .iter()
            .filter(|l| l.from == *name)
            .cloned()
            .collect())
    }

    async fn resolve(&self, name: &TypeName) -> Result<TableRef> {
        Ok(self.get_type(name).await?.table)
    }
}
