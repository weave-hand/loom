//! A read-only governance `ControlPlane` for query-api: `acl()`/`ontology()` read
//! over the engine wire; `queue()` and `lineage()` delegate to the direct Postgres
//! plane (the GC enqueue and the governed lineage read endpoints); `catalog()`/
//! `begin()` are guarded because query-api never uses them through this plane.
//! Write/define governance methods fail loudly — query-api authorizes reads here
//! and sends pre-authorized writes via the engine's write RPCs; it never defines
//! governance.

use std::sync::Arc;

use async_trait::async_trait;
use control_plane_core::{
    Acl, Action, ActionDef, ActionName, Catalog, ControlPlane, ControlPlaneError, Decision, Effect,
    Lineage, LinkDef, ObjectType, Ontology, Page, PageReq, Policy, PolicyTarget, Queue, RoleId,
    SubjectId, TableRef, Tx, TypeName, VectorIndexDef,
};
use engine_wire::client::GrpcQueueClient;

type Result<T> = std::result::Result<T, ControlPlaneError>;

fn read_only(method: &str) -> ControlPlaneError {
    ControlPlaneError::Backend(
        format!("WireControlPlane is a read-only governance client: {method} is not supported")
            .into(),
    )
}

/// ACL reads over the engine wire; write/define methods rejected.
pub struct WireAcl {
    client: GrpcQueueClient,
}

#[async_trait]
impl Acl for WireAcl {
    async fn check(
        &self,
        subject: &SubjectId,
        action: Action,
        target: &PolicyTarget,
    ) -> Result<Decision> {
        self.client.gov_check(subject, action, target).await
    }

    async fn policies_for(
        &self,
        subject: &SubjectId,
        action: Action,
        target: &PolicyTarget,
        page: PageReq,
    ) -> Result<Page<Policy>> {
        self.client
            .gov_policies_for(subject, action, target, &page)
            .await
    }

    async fn define_subject(&self, _id: &SubjectId) -> Result<()> {
        Err(read_only("define_subject"))
    }

    async fn define_role(&self, _id: &RoleId) -> Result<()> {
        Err(read_only("define_role"))
    }

    async fn assign_role(&self, _s: &SubjectId, _r: &RoleId) -> Result<()> {
        Err(read_only("assign_role"))
    }

    async fn has_role(&self, _s: &SubjectId, _r: &RoleId) -> Result<bool> {
        Err(read_only("has_role"))
    }

    async fn list_roles(&self) -> Result<Vec<RoleId>> {
        Err(read_only("list_roles"))
    }

    async fn unassign_role(&self, _s: &SubjectId, _r: &RoleId) -> Result<()> {
        Err(read_only("unassign_role"))
    }

    async fn add_role_inheritance(&self, _r: &RoleId, _i: &RoleId) -> Result<()> {
        Err(read_only("add_role_inheritance"))
    }

    async fn remove_role_inheritance(&self, _r: &RoleId, _i: &RoleId) -> Result<()> {
        Err(read_only("remove_role_inheritance"))
    }

    async fn grant(&self, _r: &RoleId, _a: Action, _t: PolicyTarget, _e: Effect) -> Result<()> {
        Err(read_only("grant"))
    }

    async fn revoke(&self, _r: &RoleId, _a: Action, _t: &PolicyTarget) -> Result<()> {
        Err(read_only("revoke"))
    }

    async fn set_policy(&self, _r: &RoleId, _a: Action, _p: Policy) -> Result<()> {
        Err(read_only("set_policy"))
    }

    async fn clear_policy(&self, _r: &RoleId, _a: Action, _t: &PolicyTarget) -> Result<()> {
        Err(read_only("clear_policy"))
    }
}

/// Ontology reads over the engine wire; write/define methods rejected.
pub struct WireOntology {
    client: GrpcQueueClient,
}

#[async_trait]
impl Ontology for WireOntology {
    async fn get_type(&self, name: &TypeName) -> Result<ObjectType> {
        self.client.gov_get_type(name).await
    }

    async fn resolve(&self, name: &TypeName) -> Result<TableRef> {
        self.client.gov_resolve(name).await
    }

    async fn links(&self, name: &TypeName, page: PageReq) -> Result<Page<LinkDef>> {
        self.client.gov_links(name, &page).await
    }

    async fn links_to(&self, name: &TypeName, page: PageReq) -> Result<Page<LinkDef>> {
        self.client.gov_links_to(name, &page).await
    }

    async fn get_action(&self, name: &ActionName) -> Result<ActionDef> {
        self.client.gov_get_action(name).await
    }

    async fn vector_indexes_for(&self, type_name: &TypeName) -> Result<Vec<VectorIndexDef>> {
        self.client.gov_vector_indexes_for(type_name).await
    }

    async fn get_vector_index(
        &self,
        type_name: &TypeName,
        name: &str,
    ) -> Result<Option<VectorIndexDef>> {
        self.client.gov_get_vector_index(type_name, name).await
    }

    async fn list_types(&self, page: PageReq) -> Result<Page<ObjectType>> {
        self.client.gov_list_types(&page).await
    }

    async fn define_type(&self, _ty: ObjectType) -> Result<()> {
        Err(read_only("define_type"))
    }

    async fn define_link(&self, _link: LinkDef) -> Result<()> {
        Err(read_only("define_link"))
    }

    async fn define_action(&self, _action: ActionDef) -> Result<()> {
        Err(read_only("define_action"))
    }

    async fn define_vector_index(&self, _def: VectorIndexDef) -> Result<()> {
        Err(read_only("define_vector_index"))
    }
}

/// Composite read-only governance plane. `queue()` delegates to `direct`.
pub struct WireControlPlane {
    acl: WireAcl,
    ontology: WireOntology,
    direct: Arc<dyn ControlPlane>,
}

impl WireControlPlane {
    #[must_use]
    pub fn new(client: GrpcQueueClient, direct: Arc<dyn ControlPlane>) -> Self {
        Self {
            acl: WireAcl {
                client: client.clone(),
            },
            ontology: WireOntology { client },
            direct,
        }
    }
}

#[async_trait]
impl ControlPlane for WireControlPlane {
    fn acl(&self) -> &(dyn Acl + Send + Sync) {
        &self.acl
    }

    fn ontology(&self) -> &(dyn Ontology + Send + Sync) {
        &self.ontology
    }

    fn queue(&self) -> &(dyn Queue + Send + Sync) {
        self.direct.queue()
    }

    #[expect(
        clippy::panic,
        reason = "read-only governance client: query-api never reads the catalog through this plane"
    )]
    fn catalog(&self) -> &(dyn Catalog + Send + Sync) {
        panic!("WireControlPlane is a read-only governance client: catalog() is not supported")
    }

    fn lineage(&self) -> &(dyn Lineage + Send + Sync) {
        // Lineage is not carried over the engine wire; read it from the direct
        // Postgres plane, exactly as `queue()` does. query-api's governed lineage
        // read endpoints resolve provenance here.
        self.direct.lineage()
    }

    async fn begin(&self) -> Result<Box<dyn Tx + Send>> {
        Err(read_only("begin"))
    }
}
