//! `/search` post-filter fault classification: an engine-derived identity cell that
//! fails coercion (server-data-integrity fault, not caller-forgeable) is an internal
//! 500 with one operator log, NOT a caller 400 echoing engine data. RE-eligible:
//! MemoryControlPlane + a stub serving engine returning a type-inconsistent hit; the
//! `tracing::error!` is captured synchronously (current-thread runtime) so no
//! multi-thread subscriber hop can drop it.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use control_plane_core::{
    Acl, Action, CompareOp, ControlPlane, Effect, ObjectType, Ontology, Policy, PolicyTarget,
    RoleId, RowFilter, ScalarValue, SubjectId, TypeName,
};
use control_plane_memory::MemoryControlPlane;
use query_api::handler::{QueryDeps, QueryError, Subject, VectorSearchQuery, vector_search};
use query_api::http::query_error_response;
use query_api::serving::{Rows, ServingEngine, ServingError, SqlValue};
use query_api::sql::{DataFusionDialect, SqlDialect};

/// A serving engine whose kNN returns one hit whose identity cell is `Text` — i.e.
/// does not coerce to a `Long`/`Integer` declared identity. `fetch_rows` is unused here.
struct BadHitServing;

#[async_trait]
impl ServingEngine for BadHitServing {
    async fn fetch_rows(&self, _sql: &str, _params: &[SqlValue]) -> Result<Rows, ServingError> {
        Err(ServingError::Engine("unused".into()))
    }
    async fn vector_search(
        &self,
        _table: &control_plane_core::TableRef,
        _index: &str,
        _query: &[f32],
        _k: usize,
        _nprobe: Option<u32>,
        _ef: Option<u32>,
    ) -> Result<Rows, ServingError> {
        // [id, distance]; id is Text -> will fail to coerce to the Long identity.
        Ok(Rows {
            columns: vec!["id".into(), "distance".into()],
            rows: vec![vec![SqlValue::Text("x".into()), SqlValue::Double(0.0)]],
        })
    }
    fn dialect(&self) -> &'static dyn SqlDialect {
        &DataFusionDialect
    }
}

/// A `Docs` type with a `Long` identity `id`, a subject with Read + a row filter on it
/// (so the post-filter actually runs), in a MemoryControlPlane.
async fn seed(cp: &MemoryControlPlane) -> SubjectId {
    let ty = ObjectType::build("Docs", ("main", "docs"))
        .prop_req("id", "Long")
        .identity("id")
        .done();
    cp.define_type(ty).await.unwrap();
    let subj = SubjectId("alice".into());
    let role = RoleId("alice-role".into());
    cp.define_subject(&subj).await.unwrap();
    cp.define_role(&role).await.unwrap();
    cp.assign_role(&subj, &role).await.unwrap();
    cp.grant(
        &role,
        Action::Read,
        PolicyTarget::Type(TypeName("Docs".into())),
        Effect::Allow,
    )
    .await
    .unwrap();
    cp.set_policy(
        &role,
        Action::Read,
        Policy {
            target: PolicyTarget::Type(TypeName("Docs".into())),
            row_filter: Some(RowFilter::Compare {
                property: "id".into(),
                op: CompareOp::Gt,
                value: ScalarValue::Int(0),
            }),
            deny_columns: vec![],
            mask_columns: vec![],
        },
    )
    .await
    .unwrap();
    subj
}

/// Capturing `MakeWriter` over a shared buffer, so the `tracing::error!` text is
/// inspectable and countable.
#[derive(Clone)]
struct CaptureWriter(Arc<Mutex<Vec<u8>>>);
impl std::io::Write for CaptureWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for CaptureWriter {
    type Writer = CaptureWriter;
    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

#[tokio::test(flavor = "current_thread")]
async fn engine_hit_coercion_fault_is_internal_500_with_one_log() {
    let cp = MemoryControlPlane::new(std::time::Duration::from_millis(300));
    let subj = seed(&cp).await;
    let serving = BadHitServing;
    let deps = QueryDeps {
        ontology: cp.ontology(),
        acl: cp.acl(),
        serving: &serving,
        default_limit: 1000,
    };
    let q = VectorSearchQuery {
        type_name: "Docs".into(),
        index_name: "by_sim".into(),
        query: vec![1.0, 0.0, 0.0, 0.0],
        k: 2,
        nprobe: None,
        ef_search: None,
    };

    // Classification: the engine-derived coercion fault is wrapped as Internal, NOT
    // surfaced as the caller-facing BadFilterValue.
    let err = vector_search(&q, &Subject(subj), &deps).await.unwrap_err();
    assert!(
        matches!(err, QueryError::Internal { .. }),
        "engine hit coercion fault must classify as Internal, got {err:?}"
    );
    // Its Display chains the wrap context and the underlying coercion detail (column).
    let shown = err.to_string();
    assert!(shown.contains("post-filter"), "context present: {shown}");
    assert!(
        shown.contains("id"),
        "coercion detail names the column: {shown}"
    );

    // Render + operator log: opaque 500 body, and exactly one ERROR event carrying the
    // detail. `query_error_response` runs synchronously inside `with_default`, so the
    // thread-local subscriber reliably captures it.
    let buf = Arc::new(Mutex::new(Vec::new()));
    let sub = tracing_subscriber::fmt()
        .with_writer(CaptureWriter(buf.clone()))
        .with_max_level(tracing::Level::ERROR)
        .with_ansi(false)
        .finish();
    let resp = tracing::subscriber::with_default(sub, || query_error_response(err, "search"));
    assert_eq!(resp.status(), axum::http::StatusCode::INTERNAL_SERVER_ERROR);
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    assert_eq!(
        &body[..],
        b"internal error",
        "opaque body, no bad_filter_value echo"
    );

    let logged = String::from_utf8(buf.lock().unwrap().clone()).unwrap();
    assert_eq!(
        logged.matches("ERROR").count(),
        1,
        "exactly one error log: {logged}"
    );
    // `internal_error` logs `error = %e`, and Internal's Display chains the wrap context
    // and the FilterError detail — so the operator log carries both.
    assert!(
        logged.contains("post-filter"),
        "log carries the wrap context: {logged}"
    );
    assert!(
        logged.contains("id"),
        "log carries the coercion detail (column): {logged}"
    );
}
