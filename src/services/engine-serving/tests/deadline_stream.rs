//! Unit tests for `DeadlineStream`: the wall-clock adapter that bounds a governed
//! SQL result stream. Pure — a hand-built `RecordBatchStreamAdapter` stands in for
//! the real DataFusion stream, so no fixture/Postgres is needed.

use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::error::DataFusionError;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use engine_serving::EngineServingError;
use engine_serving::sql_limits::{DeadlineStream, governed_stream_error};
use futures::StreamExt;

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]))
}

/// A two-batch stream of `[0]` then `[1]`, with the given schema.
fn two_batches() -> SendableRecordBatchStream {
    let s = schema();
    let mk = |v: i64| {
        RecordBatch::try_new(s.clone(), vec![Arc::new(Int64Array::from(vec![v]))]).unwrap()
    };
    let items = vec![Ok(mk(0)), Ok(mk(1))];
    Box::pin(RecordBatchStreamAdapter::new(
        s,
        futures::stream::iter(items),
    ))
}

#[tokio::test]
async fn passes_batches_through_before_the_deadline() {
    let inner = two_batches();
    let ds = DeadlineStream::until(inner, Instant::now() + Duration::from_secs(300));
    let got: Vec<_> = ds.collect().await;
    assert_eq!(got.len(), 2, "both batches should pass through");
    assert!(got.iter().all(std::result::Result::is_ok));
}

#[tokio::test]
async fn forwards_the_inner_schema() {
    let inner = two_batches();
    let ds = DeadlineStream::until(inner, Instant::now() + Duration::from_secs(300));
    use datafusion::physical_plan::RecordBatchStream as _;
    assert_eq!(ds.schema().as_ref(), schema().as_ref());
}

#[tokio::test]
async fn past_deadline_yields_resources_exhausted_then_terminates() {
    let inner = two_batches();
    // A deadline already in the past: the very first poll must trip.
    let ds = DeadlineStream::until(inner, Instant::now() - Duration::from_secs(1));
    let got: Vec<_> = ds.collect().await;
    assert_eq!(got.len(), 1, "one error item, then the stream ends");
    let err = got.into_iter().next().unwrap().unwrap_err();
    assert!(
        matches!(err, DataFusionError::ResourcesExhausted(_)),
        "expected ResourcesExhausted, got {err:?}"
    );
}

/// The load-bearing case: the inner stream never resolves, so a bare
/// `Instant::now()` check would never run a second time. Only a waker-backed timer
/// cuts this off — which is what `SortExec` (whole input consumed inside one
/// `poll_next`) actually looks like from here.
#[tokio::test]
async fn the_timer_preempts_an_inner_stream_that_never_resolves() {
    let s = schema();
    let inner: SendableRecordBatchStream = Box::pin(RecordBatchStreamAdapter::new(
        s,
        futures::stream::pending::<Result<RecordBatch, DataFusionError>>(),
    ));
    let mut ds = DeadlineStream::until(inner, Instant::now() + Duration::from_millis(50));
    let item = ds.next().await.expect("the deadline must produce an item");
    let err = item.unwrap_err();
    assert!(
        matches!(err, DataFusionError::ResourcesExhausted(_)),
        "expected ResourcesExhausted, got {err:?}"
    );
}

#[test]
fn classifier_lifts_resources_exhausted_and_nothing_else() {
    let re = DataFusionError::ResourcesExhausted("budget".into());
    assert!(matches!(
        governed_stream_error(&re),
        EngineServingError::ResourceExhausted(_)
    ));
    // Wrapped in context: `find_root` must still see through it.
    let wrapped = re.context("while sorting");
    assert!(matches!(
        governed_stream_error(&wrapped),
        EngineServingError::ResourceExhausted(_)
    ));
    let other = DataFusionError::Plan("nope".into());
    assert!(matches!(
        governed_stream_error(&other),
        EngineServingError::Engine(_)
    ));
}

#[test]
fn classifier_names_the_loom_budget_not_datafusions_config_keys() {
    // DataFusion's own OOM advice cites `datafusion.runtime.memory_limit`, which is
    // meaningless to a loom caller — the spec requires the message name OUR budget.
    let pool = DataFusionError::ResourcesExhausted(
        "Failed to allocate additional 1024 bytes for ExternalSorter[0]".into(),
    );
    let EngineServingError::ResourceExhausted(m) = governed_stream_error(&pool) else {
        panic!("expected ResourceExhausted");
    };
    assert!(m.contains("LOOM_SQL_MEMORY_LIMIT_BYTES"), "got: {m}");

    // The deadline path authors its own message and must NOT be re-prefixed as a
    // memory breach.
    let deadline = DataFusionError::ResourcesExhausted(
        "statement exceeded its wall-clock budget (LOOM_SQL_TIMEOUT_SECS)".into(),
    );
    let EngineServingError::ResourceExhausted(m) = governed_stream_error(&deadline) else {
        panic!("expected ResourceExhausted");
    };
    assert!(!m.contains("LOOM_SQL_MEMORY_LIMIT_BYTES"), "got: {m}");
    assert!(m.contains("LOOM_SQL_TIMEOUT_SECS"), "got: {m}");
}
