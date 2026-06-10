//! HTTP wiring smoke test: a GET maps into read_object and Rows serialize to JSON.
//! No socket is bound (tower oneshot); canned stubs exercise the route, not DuckDB.

use std::sync::Arc;

use async_trait::async_trait;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use control_plane_core::{
    Acl, Action, Decision, LinkDef, ObjectType, Page, PageReq, Policy, PolicyTarget, PropertyDef,
    Result, RoleId, SubjectId, TableRef, TypeName,
};
use http_body_util::BodyExt;
use query_api::http::{AppState, router};
use query_api::serving::{Rows, ServingEngine, ServingError, SqlValue};
use tower::ServiceExt;

struct StubOntology;

#[async_trait]
impl control_plane_core::Ontology for StubOntology {
    async fn define_type(&self, _ty: ObjectType) -> Result<()> {
        unimplemented!()
    }
    async fn define_link(&self, _link: LinkDef) -> Result<()> {
        unimplemented!()
    }
    async fn get_type(&self, name: &TypeName) -> Result<ObjectType> {
        assert_eq!(name.0, "Order");
        Ok(ObjectType {
            name: TypeName("Order".into()),
            properties: vec![PropertyDef {
                name: "id".into(),
                ty: "Long".into(),
                required: true,
            }],
            table: TableRef {
                schema: "main".into(),
                name: "orders".into(),
            },
        })
    }
    async fn list_types(&self, _page: PageReq) -> Result<Page<ObjectType>> {
        unimplemented!()
    }
    async fn links(&self, _name: &TypeName, _page: PageReq) -> Result<Page<LinkDef>> {
        unimplemented!()
    }
    async fn resolve(&self, _name: &TypeName) -> Result<TableRef> {
        unimplemented!()
    }
}

struct StubAcl;

#[async_trait]
impl Acl for StubAcl {
    async fn define_subject(&self, _id: &SubjectId) -> Result<()> {
        unimplemented!()
    }
    async fn define_role(&self, _id: &RoleId) -> Result<()> {
        unimplemented!()
    }
    async fn assign_role(&self, _subject: &SubjectId, _role: &RoleId) -> Result<()> {
        unimplemented!()
    }
    async fn unassign_role(&self, _subject: &SubjectId, _role: &RoleId) -> Result<()> {
        unimplemented!()
    }
    async fn grant(&self, _role: &RoleId, _action: Action, _target: PolicyTarget) -> Result<()> {
        unimplemented!()
    }
    async fn revoke(&self, _role: &RoleId, _action: Action, _target: &PolicyTarget) -> Result<()> {
        unimplemented!()
    }
    async fn set_policy(&self, _role: &RoleId, _policy: Policy) -> Result<()> {
        unimplemented!()
    }
    async fn clear_policy(&self, _role: &RoleId, _target: &PolicyTarget) -> Result<()> {
        unimplemented!()
    }
    async fn check(
        &self,
        _subject: &SubjectId,
        _action: Action,
        _target: &PolicyTarget,
    ) -> Result<Decision> {
        // Grant read access so the route reaches read_object's body in this smoke test.
        Ok(Decision::Allow)
    }
    async fn policies_for(
        &self,
        _subject: &SubjectId,
        _target: &PolicyTarget,
        _page: PageReq,
    ) -> Result<Page<Policy>> {
        Ok(Page::from_full(vec![]))
    }
}

struct StubServing;

#[async_trait]
impl ServingEngine for StubServing {
    async fn fetch_rows(
        &self,
        _sql: &str,
        _params: &[SqlValue],
    ) -> std::result::Result<Rows, ServingError> {
        Ok(Rows {
            columns: vec!["id".into()],
            rows: vec![vec![SqlValue::Int(1)]],
        })
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn get_objects_returns_json_rows() {
    let app = router(AppState {
        ontology: Arc::new(StubOntology),
        acl: Arc::new(StubAcl),
        serving: Arc::new(StubServing),
    });
    let res = app
        .oneshot(
            Request::builder()
                .uri("/objects/Order")
                .header("X-Loom-Subject", "analyst")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let body = res.into_body().collect().await.unwrap().to_bytes();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["columns"][0], "id");
    assert_eq!(json["rows"][0][0], 1);
}
