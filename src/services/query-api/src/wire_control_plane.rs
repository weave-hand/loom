//! A read-only governance `ControlPlane` for query-api: `acl()`/`ontology()` read
//! over the engine wire; `queue()`, `lineage()`, `catalog()`, and `transforms()`
//! delegate to the direct Postgres plane (the GC enqueue, governed lineage reads,
//! dataset metadata reads, and transform def/run reads for the admin routes);
//! `begin()` stays guarded because query-api never opens a control-plane
//! transaction through this plane.
//! Write/define governance methods fail loudly — query-api authorizes reads here
//! and sends pre-authorized writes via the engine's write RPCs; it never defines
//! governance.

use std::sync::Arc;

use async_trait::async_trait;
use control_plane_core::{
    Acl, Action, ActionDef, ActionName, Auth, Catalog, ControlPlane, ControlPlaneError, Decision,
    Effect, Grant, Lineage, LinkDef, ObjectType, Ontology, Page, PageReq, Policy, PolicyTarget,
    Queue, RoleId, RolePolicy, SubjectId, TableRef, Transforms, Tx, TypeName, VectorIndexDef,
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

    // Errors by design: there is no `gov_has_role` wire RPC, and the admin HTTP
    // gate (`require_admin`) reads through the direct/postgres control plane, not
    // this wire client — so this is never reached from that path.
    async fn has_role(&self, _s: &SubjectId, _r: &RoleId) -> Result<bool> {
        Err(read_only("has_role"))
    }

    // Errors by design: there is no `gov_list_roles` wire RPC; unreached from the
    // admin surface for the same reason as `has_role` above.
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

    // Errors by design: there is no `gov_list_grants` wire RPC; the management
    // read surface is served by the direct/postgres control plane, not this
    // wire client — so this is never reached from that path.
    async fn list_grants(&self, _r: &RoleId, _p: PageReq) -> Result<Page<Grant>> {
        Err(read_only("list_grants"))
    }

    // Errors by design: no `gov_roles_of` wire RPC; unreached from the
    // management surface for the same reason as `list_grants` above.
    async fn roles_of(&self, _s: &SubjectId, _p: PageReq) -> Result<Page<RoleId>> {
        Err(read_only("roles_of"))
    }

    async fn delete_role(&self, _r: &RoleId) -> Result<()> {
        Err(read_only("delete_role"))
    }

    async fn set_policy(&self, _r: &RoleId, _a: Action, _p: Policy) -> Result<()> {
        Err(read_only("set_policy"))
    }

    async fn clear_policy(&self, _r: &RoleId, _a: Action, _t: &PolicyTarget) -> Result<()> {
        Err(read_only("clear_policy"))
    }

    // Errors by design: there is no `gov_list_policies` wire RPC; the management
    // read surface is served by the direct/postgres control plane, not this
    // wire client — so this is never reached from that path.
    async fn list_policies(&self, _r: &RoleId, _p: PageReq) -> Result<Page<RolePolicy>> {
        Err(read_only("list_policies"))
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

    async fn list_actions(&self, page: PageReq) -> Result<Page<ActionDef>> {
        self.client.gov_list_actions(&page).await
    }

    async fn define_type(&self, _ty: ObjectType) -> Result<()> {
        Err(read_only("define_type"))
    }

    async fn define_link(&self, _link: LinkDef) -> Result<()> {
        Err(read_only("define_link"))
    }

    async fn delete_link(&self, _from: &TypeName, _name: &str) -> Result<()> {
        Err(read_only("delete_link"))
    }

    // The referrer guard runs control-plane-side (postgres/memory `delete_link`);
    // this wire client doesn't manage links (define_link/delete_link are read_only),
    // so this guard helper is never invoked here — surface it as unsupported.
    async fn derived_properties_referencing(
        &self,
        _from: &TypeName,
        _name: &str,
    ) -> Result<Vec<String>> {
        Err(read_only("derived_properties_referencing"))
    }

    async fn define_action(&self, _action: ActionDef) -> Result<()> {
        Err(read_only("define_action"))
    }

    async fn delete_action(&self, _name: &ActionName) -> Result<()> {
        Err(read_only("delete_action"))
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

    fn auth(&self) -> &(dyn Auth + Send + Sync) {
        self.direct.auth()
    }

    fn catalog(&self) -> &(dyn Catalog + Send + Sync) {
        // Catalog metadata reads are not carried over the engine wire; serve them
        // from the direct Postgres plane, exactly as `queue()` and `lineage()` do.
        // query-api's dataset read endpoints resolve table metadata here.
        self.direct.catalog()
    }

    fn lineage(&self) -> &(dyn Lineage + Send + Sync) {
        // Lineage is not carried over the engine wire; read it from the direct
        // Postgres plane, exactly as `queue()` does. query-api's governed lineage
        // read endpoints resolve provenance here.
        self.direct.lineage()
    }

    fn transforms(&self) -> &(dyn Transforms + Send + Sync) {
        // Transform defs/runs are not carried over the engine wire; read them from
        // the direct Postgres plane, exactly as `queue()` and `lineage()` do. The
        // admin transform routes resolve def/run metadata here.
        self.direct.transforms()
    }

    async fn begin(&self) -> Result<Box<dyn Tx + Send>> {
        Err(read_only("begin"))
    }
}
