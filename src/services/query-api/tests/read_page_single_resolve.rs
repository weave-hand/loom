//! read_object_page must resolve governance ONCE per request: exactly one acl.check and
//! one policies_for for the queried type. (Before the governed-read spine it ran its own
//! prologue and then compile_object_read ran it again — 2x acl.check / 2x get_type /
//! 2x policies_for per paginated read.)

use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use control_plane_core::{
    Acl, Action, Decision, Effect, ObjectType, Ontology, Page, PageReq, Policy, PolicyTarget,
    PropertyDef, Result as CpResult, RoleId, SubjectId, TableRef, TypeName,
};
use control_plane_memory::MemoryControlPlane;
use query_api::handler::{ObjectQuery, QueryDeps, Subject, read_object_page};
use query_api::serving::{Rows, ServingEngine, ServingError, SqlValue};

/// Delegates every Acl method to the memory control plane, counting the two read-path
/// calls. The counts prove the paginated read resolves governance once.
struct CountingAcl<'a> {
    inner: &'a MemoryControlPlane,
    checks: AtomicUsize,
    loads: AtomicUsize,
}

#[async_trait]
impl Acl for CountingAcl<'_> {
    async fn define_subject(&self, id: &SubjectId) -> CpResult<()> {
        self.inner.define_subject(id).await
    }
    async fn define_role(&self, id: &RoleId) -> CpResult<()> {
        self.inner.define_role(id).await
    }
    async fn assign_role(&self, subject: &SubjectId, role: &RoleId) -> CpResult<()> {
        self.inner.assign_role(subject, role).await
    }
    async fn unassign_role(&self, subject: &SubjectId, role: &RoleId) -> CpResult<()> {
        self.inner.unassign_role(subject, role).await
    }
    async fn add_role_inheritance(&self, role: &RoleId, inherits: &RoleId) -> CpResult<()> {
        self.inner.add_role_inheritance(role, inherits).await
    }
    async fn remove_role_inheritance(&self, role: &RoleId, inherits: &RoleId) -> CpResult<()> {
        self.inner.remove_role_inheritance(role, inherits).await
    }
    async fn grant(
        &self,
        role: &RoleId,
        action: Action,
        target: PolicyTarget,
        effect: Effect,
    ) -> CpResult<()> {
        self.inner.grant(role, action, target, effect).await
    }
    async fn revoke(&self, role: &RoleId, action: Action, target: &PolicyTarget) -> CpResult<()> {
        self.inner.revoke(role, action, target).await
    }
    async fn set_policy(&self, role: &RoleId, action: Action, policy: Policy) -> CpResult<()> {
        self.inner.set_policy(role, action, policy).await
    }
    async fn clear_policy(
        &self,
        role: &RoleId,
        action: Action,
        target: &PolicyTarget,
    ) -> CpResult<()> {
        self.inner.clear_policy(role, action, target).await
    }
    async fn check(
        &self,
        subject: &SubjectId,
        action: Action,
        target: &PolicyTarget,
    ) -> CpResult<Decision> {
        self.checks.fetch_add(1, Ordering::SeqCst);
        self.inner.check(subject, action, target).await
    }
    async fn policies_for(
        &self,
        subject: &SubjectId,
        action: Action,
        target: &PolicyTarget,
        page: PageReq,
    ) -> CpResult<Page<Policy>> {
        self.loads.fetch_add(1, Ordering::SeqCst);
        self.inner.policies_for(subject, action, target, page).await
    }
}

/// Serves 3 canned [id, name] rows regardless of SQL (limit is applied by the engine in
/// production; here the over-fetch row count exercises the keyset truncation).
struct PageServing;

#[async_trait]
impl ServingEngine for PageServing {
    async fn fetch_rows(&self, _sql: &str, _params: &[SqlValue]) -> Result<Rows, ServingError> {
        Ok(Rows {
            columns: vec!["id".into(), "name".into()],
            rows: vec![
                vec![SqlValue::Int(1), SqlValue::Text("a".into())],
                vec![SqlValue::Int(2), SqlValue::Text("b".into())],
                vec![SqlValue::Int(3), SqlValue::Text("c".into())],
            ],
        })
    }
}

fn person() -> ObjectType {
    ObjectType {
        name: TypeName("Person".into()),
        properties: vec![
            PropertyDef {
                name: "id".into(),
                ty: "Long".into(),
                required: true,
                constraints: control_plane_core::PropertyConstraints::default(),
            },
            PropertyDef {
                name: "name".into(),
                ty: "String".into(),
                required: false,
                constraints: control_plane_core::PropertyConstraints::default(),
            },
        ],
        derived: vec![],
        table: TableRef {
            schema: "main".into(),
            name: "person".into(),
        },
        identity: Some("id".into()),
    }
}

#[tokio::test]
async fn paginated_read_resolves_governance_exactly_once() {
    let cp = MemoryControlPlane::new(Duration::from_millis(300));
    cp.define_type(person()).await.unwrap();
    let analyst = SubjectId("analyst".into());
    let reader = RoleId("reader".into());
    cp.define_subject(&analyst).await.unwrap();
    cp.define_role(&reader).await.unwrap();
    cp.assign_role(&analyst, &reader).await.unwrap();
    cp.grant(
        &reader,
        Action::Read,
        PolicyTarget::Type(TypeName("Person".into())),
        Effect::Allow,
    )
    .await
    .unwrap();

    let acl = CountingAcl {
        inner: &cp,
        checks: AtomicUsize::new(0),
        loads: AtomicUsize::new(0),
    };
    let serving = PageServing;
    let deps = QueryDeps {
        ontology: &cp,
        acl: &acl,
        serving: &serving,
        default_limit: 100,
    };
    let q = ObjectQuery {
        type_name: "Person".into(),
        filters: vec![],
        ids: vec![],
        or_raw: vec![],
    };
    let (rows, next) = read_object_page(&q, &Subject(analyst), &deps, 2, None)
        .await
        .unwrap();
    assert_eq!(rows.rows.len(), 2);
    assert!(next.is_some());
    assert_eq!(
        acl.checks.load(Ordering::SeqCst),
        1,
        "read_object_page must run the coarse Read gate exactly once"
    );
    assert_eq!(
        acl.loads.load(Ordering::SeqCst),
        1,
        "read_object_page must load the read policy exactly once"
    );
}
