//! POST /tables/{schema}/{table}/compact enqueues a compact_table job and returns its id.

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use control_plane_core::{COMPACT_JOB_KIND, ControlPlane, SnapshotId};
use control_plane_postgres::fixture::PgFixture;
use ingest::http::{AppState, router};
use ingest::landing::{LandRequest, LandingMaterializer};
use tower::ServiceExt;

/// Stub materializer — the compact endpoint never touches it, so all methods
/// are unreachable. Satisfies the trait bound on `AppState.materializer`.
struct StubMaterializer;

#[async_trait::async_trait]
impl LandingMaterializer for StubMaterializer {
    async fn land(&self, _req: LandRequest<'_>) -> Result<SnapshotId, ingest::IngestError> {
        unreachable!("compact endpoint does not call materializer")
    }
}

fn stub_materializer() -> Arc<dyn LandingMaterializer> {
    Arc::new(StubMaterializer)
}

#[tokio::test(flavor = "multi_thread")]
async fn compact_endpoint_enqueues_job() {
    let fx = PgFixture::shared();
    let (cp, _db) = fx.fresh_db().await;
    let cp = Arc::new(cp);

    let state = AppState {
        materializer: stub_materializer(),
        cp: cp.clone() as Arc<dyn ControlPlane>,
    };
    let app = router(state);

    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/tables/main/orders/compact")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::ACCEPTED);

    // The job is now dequeueable.
    let job = cp
        .queue()
        .dequeue(&[COMPACT_JOB_KIND.to_string()], "test")
        .await
        .unwrap();
    let job = job.expect("a compact_table job was enqueued");
    assert_eq!(job.kind, COMPACT_JOB_KIND);
    let payload: serde_json::Value = job.payload;
    assert_eq!(payload["schema"], "main");
    assert_eq!(payload["name"], "orders");
}
