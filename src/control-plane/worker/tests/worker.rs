use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use control_plane_core::{JobFailure, NewJob, Queue, RetryPolicy};
use control_plane_memory::MemoryControlPlane;
use control_plane_worker::Worker;
use tokio_util::sync::CancellationToken;
use tracing_test::traced_test;

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

    let worker =
        Worker::new(cp.clone(), "w1", LOCK_TIMEOUT).with_poll_interval(Duration::from_millis(50));
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

    let worker =
        Worker::new(cp.clone(), "w1", LOCK_TIMEOUT).with_poll_interval(Duration::from_millis(50));
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

    let worker =
        Worker::new(cp.clone(), "w1", LOCK_TIMEOUT).with_poll_interval(Duration::from_millis(50));
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

// A handler that runs longer than lock_timeout is NOT reclaimed: the worker
// heartbeats the lease, so the job runs exactly once and no other worker can
// steal it mid-flight. Without heartbeating this job would be double-executed.
#[tokio::test]
async fn heartbeat_keeps_long_handler_single() {
    let cp = MemoryControlPlane::new(LOCK_TIMEOUT); // 300ms lease
    cp.enqueue(job(KIND)).await.unwrap();

    let runs = Arc::new(AtomicU32::new(0));
    let r = runs.clone();
    let started = Arc::new(tokio::sync::Notify::new());
    let started_w = started.clone();

    let token = CancellationToken::new();
    let t = token.clone();
    let worker = Worker::new(cp.clone(), "w1", LOCK_TIMEOUT);
    let handle = tokio::spawn(async move {
        worker
            .run(&[KIND.to_string()], t, move |_job| {
                let r = r.clone();
                let started_w = started_w.clone();
                async move {
                    r.fetch_add(1, Ordering::SeqCst);
                    started_w.notify_one();
                    // Run well past LOCK_TIMEOUT so an un-heartbeated lease would expire.
                    tokio::time::sleep(LOCK_TIMEOUT * 3).await;
                    Ok(())
                }
            })
            .await
    });

    // Once the handler is in flight, a different worker must NOT be able to claim
    // the job: the lease is held by heartbeating, even though we're already past
    // LOCK_TIMEOUT relative to the original claim by the time we check.
    started.notified().await;
    tokio::time::sleep(LOCK_TIMEOUT + Duration::from_millis(100)).await;
    assert!(
        cp.dequeue(&[KIND.to_string()], "intruder")
            .await
            .unwrap()
            .is_none(),
        "lease held by heartbeat: another worker cannot reclaim the in-flight job"
    );

    // Let the handler finish and the worker drain, then shut down.
    tokio::time::sleep(LOCK_TIMEOUT * 3).await;
    token.cancel();
    handle.await.unwrap().unwrap();

    assert_eq!(runs.load(Ordering::SeqCst), 1, "handler ran exactly once");
    assert!(
        cp.dequeue(&[KIND.to_string()], "probe")
            .await
            .unwrap()
            .is_none(),
        "job completed (removed)"
    );
}

// A panicking handler is contained: that job is Abandoned and the worker survives to
// process other jobs, instead of the panic tearing down the run loop. (The panic
// message is printed to stderr by the default hook before being caught — expected noise.)
#[tokio::test]
async fn handler_panic_is_contained() {
    let cp = MemoryControlPlane::new(LOCK_TIMEOUT);
    cp.enqueue(job("boom")).await.unwrap();
    cp.enqueue(job("ok")).await.unwrap();

    let ok_ran = Arc::new(AtomicU32::new(0));
    let r = ok_ran.clone();
    let token = CancellationToken::new();
    let t = token.clone();

    let worker =
        Worker::new(cp.clone(), "w1", LOCK_TIMEOUT).with_poll_interval(Duration::from_millis(50));
    let handle = tokio::spawn(async move {
        worker
            .run(&["boom".to_string(), "ok".to_string()], t, move |j| {
                let r = r.clone();
                async move {
                    if j.kind == "boom" {
                        panic!("handler blew up");
                    }
                    r.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }
            })
            .await
    });

    tokio::time::sleep(Duration::from_millis(300)).await;
    token.cancel();
    let run_result = handle.await.unwrap();
    assert!(
        run_result.is_ok(),
        "worker survived the panic (run returned Ok)"
    );
    assert_eq!(
        ok_ran.load(Ordering::SeqCst),
        1,
        "the non-panicking job was processed"
    );
    assert!(
        cp.dequeue(&["boom".to_string(), "ok".to_string()], "probe")
            .await
            .unwrap()
            .is_none(),
        "panicking job abandoned (not retried/stuck); good job completed"
    );
}

// The worker's tracing instrumentation actually fires: a contained handler panic
// emits the "handler panic contained" warn event. Proves the tracing facade is
// wired end-to-end (catches #[instrument]/event mis-wiring that compiles to nothing).
#[tokio::test]
#[traced_test]
async fn emits_tracing_event_on_contained_panic() {
    let cp = MemoryControlPlane::new(LOCK_TIMEOUT);
    cp.enqueue(job("boom")).await.unwrap();

    let token = CancellationToken::new();
    let t = token.clone();
    let worker =
        Worker::new(cp.clone(), "w1", LOCK_TIMEOUT).with_poll_interval(Duration::from_millis(50));
    let handle = tokio::spawn(async move {
        worker
            .run(&["boom".to_string()], t, move |_j| async move {
                panic!("handler blew up");
                #[allow(unreachable_code)]
                Ok(())
            })
            .await
    });

    tokio::time::sleep(Duration::from_millis(300)).await;
    token.cancel();
    handle.await.unwrap().unwrap();

    assert!(
        logs_contain("handler panic contained"),
        "worker should emit the panic-contained tracing event"
    );
}
