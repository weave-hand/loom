//! /objects/{type}/changes gate + error surface, over the memory control plane
//! and a default (no-feed) ServingEngine -- the pre-stream failure paths that
//! need no Postgres:
//!   * 403 before existence for an ungranted subject (deny-before-existence);
//!   * 404 for a granted-but-unknown type;
//!   * 400 for a malformed cursor and for a cursor minted for another type;
//!   * 400 for ?fields= naming an unknown/denied column;
//!   * 501 when the engine does not serve the feed (the production wire client
//!     until the engine-wire hop lands -- see FUTURE).
//!
//! rust_test (no fixture).

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use control_plane_core::{
    Acl, Action, ActionDef, ActionName, Auth, Catalog, ControlPlane, ControlPlaneError, Effect,
    Lineage, LinkDef, ObjectType, Ontology, Page, PageReq, PolicyTarget, PropertyDef, Queue,
    RoleId, SubjectId, TableRef, Transforms, Tx, TypeName, VectorIndexDef,
};
use control_plane_memory::MemoryControlPlane;
use http_body_util::BodyExt;
use query_api::http::{AppState, router};
use query_api::serving::{ActionEngine, Rows, ServingEngine, ServingError, SqlValue};
use service_runtime::Subject;
use tower::ServiceExt;

struct StubServing;

#[async_trait]
impl ServingEngine for StubServing {
    async fn fetch_rows(
        &self,
        _sql: &str,
        _params: &[SqlValue],
        _at: Option<control_plane_core::SnapshotId>,
    ) -> Result<Rows, ServingError> {
        Ok(Rows::default())
    }
    // changelog_latest / changelog_feed / await_changelog keep the trait's
    // default `Err(Unsupported)` bodies -- this stub serves no feed.
}

struct StubAction;

#[async_trait]
impl ActionEngine for StubAction {
    async fn write_object(
        &self,
        _table: &TableRef,
        _columns: &[String],
        _values: &[SqlValue],
        _logical_types: &[String],
        _event: control_plane_core::LineageEvent,
        _jobs: &[control_plane_core::NewJob],
    ) -> std::result::Result<control_plane_core::SnapshotId, ServingError> {
        Ok(control_plane_core::SnapshotId(0))
    }

    async fn overwrite_table(
        &self,
        _table: &TableRef,
        _columns: &[String],
        _rows: &[Vec<SqlValue>],
        _logical_types: &[String],
        _event: control_plane_core::LineageEvent,
        _jobs: &[control_plane_core::NewJob],
    ) -> std::result::Result<control_plane_core::SnapshotId, ServingError> {
        Err(ServingError::Engine("overwrite_table unsupported".into()))
    }
}

/// An `Ontology` that resolves ONLY the `widget` type it is constructed with;
/// every other name (including a type granted -- but never defined here, e.g.
/// "Nope") is genuinely `NotFound`. This decouples "what the real
/// `MemoryControlPlane` needed to exist for `grant()` to validate" from "what
/// this test's read path sees" -- the same trick `resolve_governed.rs`'s
/// `MissingType` stub uses, letting the HTTP-level test exercise the
/// granted-but-unknown-type 404 without a `delete_type` API. Every method
/// beyond `get_type` is unreachable on the `/objects/{type}/changes` path.
struct GatedOntology {
    widget: ObjectType,
}

#[async_trait]
impl Ontology for GatedOntology {
    async fn get_type(&self, name: &TypeName) -> control_plane_core::Result<ObjectType> {
        if name.0 == self.widget.name.0 {
            Ok(self.widget.clone())
        } else {
            Err(ControlPlaneError::NotFound(format!("type {}", name.0)))
        }
    }
    async fn define_type(&self, _ty: ObjectType) -> control_plane_core::Result<()> {
        unreachable!("not exercised by the changes route")
    }
    async fn define_link(&self, _link: LinkDef) -> control_plane_core::Result<()> {
        unreachable!("not exercised by the changes route")
    }
    async fn delete_link(&self, _from: &TypeName, _name: &str) -> control_plane_core::Result<()> {
        unreachable!("not exercised by the changes route")
    }
    async fn derived_properties_referencing(
        &self,
        _from: &TypeName,
        _name: &str,
    ) -> control_plane_core::Result<Vec<String>> {
        unreachable!("not exercised by the changes route")
    }
    async fn list_types(&self, _page: PageReq) -> control_plane_core::Result<Page<ObjectType>> {
        unreachable!("not exercised by the changes route")
    }
    async fn links(
        &self,
        _name: &TypeName,
        _page: PageReq,
    ) -> control_plane_core::Result<Page<LinkDef>> {
        unreachable!("not exercised by the changes route")
    }
    async fn links_to(
        &self,
        _name: &TypeName,
        _page: PageReq,
    ) -> control_plane_core::Result<Page<LinkDef>> {
        unreachable!("not exercised by the changes route")
    }
    async fn resolve(&self, _name: &TypeName) -> control_plane_core::Result<TableRef> {
        unreachable!("not exercised by the changes route")
    }
    async fn define_action(&self, _action: ActionDef) -> control_plane_core::Result<()> {
        unreachable!("not exercised by the changes route")
    }
    async fn delete_action(&self, _name: &ActionName) -> control_plane_core::Result<()> {
        unreachable!("not exercised by the changes route")
    }
    async fn get_action(&self, _name: &ActionName) -> control_plane_core::Result<ActionDef> {
        unreachable!("not exercised by the changes route")
    }
    async fn list_actions(&self, _page: PageReq) -> control_plane_core::Result<Page<ActionDef>> {
        unreachable!("not exercised by the changes route")
    }
    async fn define_vector_index(&self, _def: VectorIndexDef) -> control_plane_core::Result<()> {
        unreachable!("not exercised by the changes route")
    }
    async fn get_vector_index(
        &self,
        _type_name: &TypeName,
        _name: &str,
    ) -> control_plane_core::Result<Option<VectorIndexDef>> {
        unreachable!("not exercised by the changes route")
    }
    async fn vector_indexes_for(
        &self,
        _type_name: &TypeName,
    ) -> control_plane_core::Result<Vec<VectorIndexDef>> {
        unreachable!("not exercised by the changes route")
    }
}

/// A `ControlPlane` composing the real memory adapter (acl/catalog/lineage/queue/
/// transforms/auth/begin) with the gated ontology above -- so a subject can hold
/// a valid grant on a type name the read path still cannot resolve.
struct GatedControlPlane {
    mem: MemoryControlPlane,
    ontology: GatedOntology,
}

#[async_trait]
impl ControlPlane for GatedControlPlane {
    fn catalog(&self) -> &(dyn Catalog + Send + Sync) {
        self.mem.catalog()
    }
    fn ontology(&self) -> &(dyn Ontology + Send + Sync) {
        &self.ontology
    }
    fn acl(&self) -> &(dyn Acl + Send + Sync) {
        self.mem.acl()
    }
    fn lineage(&self) -> &(dyn Lineage + Send + Sync) {
        self.mem.lineage()
    }
    fn queue(&self) -> &(dyn Queue + Send + Sync) {
        self.mem.queue()
    }
    fn transforms(&self) -> &(dyn Transforms + Send + Sync) {
        self.mem.transforms()
    }
    fn auth(&self) -> &(dyn Auth + Send + Sync) {
        self.mem.auth()
    }
    async fn begin(&self) -> control_plane_core::Result<Box<dyn Tx + Send>> {
        self.mem.begin().await
    }
}

fn widget_type() -> ObjectType {
    ObjectType {
        name: TypeName("Widget".into()),
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
            name: "widget".into(),
        },
        identity: Some("id".into()),
        version: None,
    }
}

/// Seed the real memory control plane: "Widget" (granted to "reader") and
/// "Nope" (granted to "reader_all", but never registered in `GatedOntology`
/// -- so it is 404 on read despite the grant). "stranger" is defined with no
/// grant at all.
async fn seeded() -> GatedControlPlane {
    let mem = MemoryControlPlane::new(Duration::from_millis(300));
    mem.define_type(widget_type()).await.unwrap();
    mem.define_type(ObjectType {
        name: TypeName("Nope".into()),
        properties: vec![],
        derived: vec![],
        table: TableRef {
            schema: "main".into(),
            name: "nope".into(),
        },
        identity: None,
        version: None,
    })
    .await
    .unwrap();

    let reader = RoleId("reader".into());
    let reader_all = RoleId("reader_all".into());
    mem.define_role(&reader).await.unwrap();
    mem.define_role(&reader_all).await.unwrap();
    mem.grant(
        &reader,
        Action::Read,
        PolicyTarget::Type(TypeName("Widget".into())),
        Effect::Allow,
    )
    .await
    .unwrap();
    mem.grant(
        &reader_all,
        Action::Read,
        PolicyTarget::Type(TypeName("Nope".into())),
        Effect::Allow,
    )
    .await
    .unwrap();

    for (name, role) in [
        ("stranger", None),
        ("reader", Some(&reader)),
        ("reader_all", Some(&reader_all)),
    ] {
        let subject = SubjectId(name.into());
        mem.define_subject(&subject).await.unwrap();
        if let Some(role) = role {
            mem.assign_role(&subject, role).await.unwrap();
        }
    }

    GatedControlPlane {
        mem,
        ontology: GatedOntology {
            widget: widget_type(),
        },
    }
}

async fn get(uri: &str, subject: &str) -> (StatusCode, String) {
    let app = router(AppState {
        cp: Arc::new(seeded().await),
        serving: Arc::new(StubServing),
        action_engine: Arc::new(StubAction),
        default_limit: 1000,
        gc_retention: std::time::Duration::from_secs(7 * 24 * 3600),
        naming: query_api::lineage_filter::local_naming(),
    });
    let mut req = Request::builder().uri(uri).body(Body::empty()).unwrap();
    req.extensions_mut()
        .insert(Subject(SubjectId(subject.into())));
    let res = app.oneshot(req).await.unwrap();
    let status = res.status();
    let body = res.into_body().collect().await.unwrap().to_bytes();
    (status, String::from_utf8_lossy(&body).into_owned())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn changes_route_gates_and_maps_errors() {
    // 403: no grant, existence not revealed.
    let (status, _) = get("/objects/Widget/changes", "stranger").await;
    assert_eq!(status, StatusCode::FORBIDDEN);

    // 404: granted subject, unknown type.
    let (status, _) = get("/objects/Nope/changes", "reader_all").await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    // 400: malformed cursor (fails before any engine call).
    let (status, _) = get("/objects/Widget/changes?cursor=%21%21", "reader").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    // 400: cursor minted for another type.
    let alien = query_api::subscribe::SubscribeCursor {
        t: "Other".into(),
        b: Default::default(),
    }
    .to_opaque();
    let (status, _) = get(&format!("/objects/Widget/changes?cursor={alien}"), "reader").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    // 400: unknown ?fields= column.
    let (status, _) = get("/objects/Widget/changes?fields=nope", "reader").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    // 501: the default engine serves no feed (cursor omitted => earliest, which
    // probes changelog_latest first).
    let (status, _) = get("/objects/Widget/changes", "reader").await;
    assert_eq!(status, StatusCode::NOT_IMPLEMENTED);
}
