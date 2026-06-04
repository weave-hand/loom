use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use control_plane_core::{JobFailure, NewJob, Queue, RetryPolicy};
use control_plane_memory::MemoryControlPlane;
use control_plane_worker::Worker;
use tokio_util::sync::CancellationToken;

fn job(kind: &str) -> NewJob {
    NewJob {
        kind: kind.into(),
        payload: serde_json::json!({}),
        run_at: None,
        priority: 0,
    }
}

const KIND: &str = "t";
const LOCK_TIMEOUT: Duration = Duration::from_millis(300);

// All enqueued jobs get processed, then the worker shuts down on cancel.
#[tokio::test]
async fn drains_jobs_then_shuts_down() {
    let cp = MemoryControlPlane::new(LOCK_TIMEOUT);
    for _ in 0..5 {
        cp.enqueue(job(KIND)).await.unwrap();
    }
    let count = Arc::new(AtomicU32::new(0));
    let c = count.clone();
    let token = CancellationToken::new();
    let t = token.clone();

    let worker = Worker::new(cp.clone(), "w1").with_poll_interval(Duration::from_millis(50));
    let handle = tokio::spawn(async move {
        worker
            .run(&[KIND.to_string()], t, move |_job| {
                let c = c.clone();
                async move {
                    c.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }
            })
            .await
    });

    tokio::time::sleep(Duration::from_millis(300)).await;
    token.cancel();
    handle.await.unwrap().unwrap();
    assert_eq!(count.load(Ordering::SeqCst), 5);
    assert!(
        cp.dequeue(&[KIND.to_string()], "probe")
            .await
            .unwrap()
            .is_none(),
        "all jobs completed (removed)"
    );
}

// A handler that fails once (Retry) then succeeds; the job is retried.
#[tokio::test]
async fn retry_then_succeed() {
    let cp = MemoryControlPlane::new(LOCK_TIMEOUT);
    cp.enqueue(job(KIND)).await.unwrap();
    let attempts = Arc::new(AtomicU32::new(0));
    let a = attempts.clone();
    let token = CancellationToken::new();
    let t = token.clone();

    let worker = Worker::new(cp.clone(), "w1").with_poll_interval(Duration::from_millis(50));
    let handle = tokio::spawn(async move {
        worker
            .run(&[KIND.to_string()], t, move |_job| {
                let a = a.clone();
                async move {
                    if a.fetch_add(1, Ordering::SeqCst) == 0 {
                        Err(JobFailure {
                            error: "first try fails".into(),
                            policy: RetryPolicy::Retry {
                                delay: Duration::from_millis(50),
                            },
                        })
                    } else {
                        Ok(())
                    }
                }
            })
            .await
    });

    tokio::time::sleep(Duration::from_millis(400)).await;
    token.cancel();
    handle.await.unwrap().unwrap();
    assert_eq!(
        attempts.load(Ordering::SeqCst),
        2,
        "failed once, then succeeded"
    );
    assert!(
        cp.dequeue(&[KIND.to_string()], "probe")
            .await
            .unwrap()
            .is_none(),
        "job eventually completed"
    );
}

// An always-failing handler that Abandons ends terminal: it is never re-run.
#[tokio::test]
async fn abandon_is_terminal() {
    let cp = MemoryControlPlane::new(LOCK_TIMEOUT);
    cp.enqueue(job(KIND)).await.unwrap();
    let attempts = Arc::new(AtomicU32::new(0));
    let a = attempts.clone();
    let token = CancellationToken::new();
    let t = token.clone();

    let worker = Worker::new(cp.clone(), "w1").with_poll_interval(Duration::from_millis(50));
    let handle = tokio::spawn(async move {
        worker
            .run(&[KIND.to_string()], t, move |_job| {
                let a = a.clone();
                async move {
                    a.fetch_add(1, Ordering::SeqCst);
                    Err(JobFailure {
                        error: "always".into(),
                        policy: RetryPolicy::Abandon,
                    })
                }
            })
            .await
    });

    tokio::time::sleep(Duration::from_millis(300)).await;
    token.cancel();
    handle.await.unwrap().unwrap();
    assert_eq!(
        attempts.load(Ordering::SeqCst),
        1,
        "abandoned job runs exactly once"
    );
}
