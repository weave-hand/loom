//! Backend-agnostic contract tests for the control-plane traits.
//! Each adapter runs the same suite against its own implementation.

use std::time::Duration;

use control_plane_core::{ControlPlane, NewJob, Queue, RetryPolicy};
use time::OffsetDateTime;

fn job(kind: &str) -> NewJob {
    NewJob {
        kind: kind.into(),
        payload: serde_json::json!({}),
        run_at: None,
        priority: 0,
    }
}

/// Contract for the `Queue` ops including transactional `enqueue`.
/// `cp` must be freshly empty; `lock_timeout` must match the adapter's configured
/// value so the reclaim assertion is timed correctly.
pub async fn queue_contract<CP: ControlPlane + Queue>(cp: &CP, lock_timeout: Duration) {
    let k = vec!["t".to_string()];
    let w = "worker-1";

    // enqueue -> dequeue (attempts=1) -> complete deletes
    let id = cp.enqueue(job("t")).await.expect("enqueue");
    let j = cp.dequeue(&k, w).await.expect("dequeue").expect("a job");
    assert_eq!(j.id, id);
    assert_eq!(j.attempts, 1);
    cp.complete(id).await.expect("complete");
    assert!(
        cp.dequeue(&k, w).await.unwrap().is_none(),
        "completed job is gone"
    );

    // priority: higher first
    cp.enqueue(NewJob {
        priority: 1,
        ..job("t")
    })
    .await
    .unwrap();
    let hi = cp
        .enqueue(NewJob {
            priority: 5,
            ..job("t")
        })
        .await
        .unwrap();
    assert_eq!(
        cp.dequeue(&k, w).await.unwrap().unwrap().id,
        hi,
        "higher priority first"
    );
    cp.complete(hi).await.unwrap();
    cp.complete(cp.dequeue(&k, w).await.unwrap().unwrap().id)
        .await
        .unwrap();

    // future run_at is not eligible
    let future = OffsetDateTime::now_utc() + Duration::from_secs(3600);
    cp.enqueue(NewJob {
        run_at: Some(future),
        ..job("t")
    })
    .await
    .unwrap();
    assert!(
        cp.dequeue(&k, w).await.unwrap().is_none(),
        "future job not eligible"
    );

    // fail + Retry reschedules; attempts increments
    let r = cp.enqueue(job("t")).await.unwrap();
    assert_eq!(cp.dequeue(&k, w).await.unwrap().unwrap().attempts, 1);
    cp.fail(
        r,
        "boom",
        RetryPolicy::Retry {
            delay: Duration::from_millis(150),
        },
    )
    .await
    .unwrap();
    assert!(
        cp.dequeue(&k, w).await.unwrap().is_none(),
        "retry is delayed"
    );
    tokio::time::sleep(Duration::from_millis(220)).await;
    let again = cp
        .dequeue(&k, w)
        .await
        .unwrap()
        .expect("retry eligible after delay");
    assert_eq!(again.attempts, 2);
    cp.complete(again.id).await.unwrap();

    // fail + Abandon -> terminal
    let a = cp.enqueue(job("t")).await.unwrap();
    cp.dequeue(&k, w).await.unwrap().unwrap();
    cp.fail(a, "dead", RetryPolicy::Abandon).await.unwrap();
    assert!(
        cp.dequeue(&k, w).await.unwrap().is_none(),
        "abandoned never returns"
    );

    // expired-lock reclaim
    let e = cp.enqueue(job("t")).await.unwrap();
    assert_eq!(cp.dequeue(&k, w).await.unwrap().unwrap().id, e);
    assert!(
        cp.dequeue(&k, w).await.unwrap().is_none(),
        "locked, not yet reclaimed"
    );
    tokio::time::sleep(lock_timeout + Duration::from_millis(50)).await;
    let reclaimed = cp
        .dequeue(&k, "worker-2")
        .await
        .unwrap()
        .expect("expired lock reclaimed");
    assert_eq!(reclaimed.id, e);
    assert_eq!(reclaimed.attempts, 2);

    // heartbeat keeps the lock fresh, then complete
    cp.heartbeat(e).await.unwrap();
    cp.complete(e).await.unwrap();

    // transactional enqueue: rolled back -> never dequeued; committed -> dequeued
    let mut tx = cp.begin().await.unwrap();
    tx.enqueue(job("tx")).await.unwrap();
    tx.rollback().await.unwrap();
    assert!(
        cp.dequeue(&["tx".into()], w).await.unwrap().is_none(),
        "rolled-back enqueue is invisible"
    );

    let mut tx = cp.begin().await.unwrap();
    let enqueued = tx.enqueue(job("tx")).await.unwrap();
    tx.commit().await.unwrap();
    let committed = cp
        .dequeue(&["tx".into()], w)
        .await
        .unwrap()
        .expect("committed enqueue is visible");
    assert_eq!(
        committed.id, enqueued,
        "dequeued job has the id returned by Tx::enqueue"
    );
    cp.complete(committed.id).await.unwrap();
}

/// Contract for `Queue::await_jobs`. Takes `cp` by value (must be `Clone + Send +
/// Sync + 'static`) so the test can hold one handle in a spawned waiter and use
/// another to enqueue. Both adapters satisfy these bounds.
pub async fn await_jobs_contract<CP>(cp: CP)
where
    CP: ControlPlane + Queue + Clone + Send + Sync + 'static,
{
    let k = vec!["w".to_string()];

    // (a) idle: returns cleanly at/after the timeout (no job ever arrives).
    let t0 = std::time::Instant::now();
    cp.await_jobs(&k, Duration::from_millis(150))
        .await
        .expect("await_jobs returns Ok on timeout");
    let idle = t0.elapsed();
    assert!(
        idle >= Duration::from_millis(120) && idle < Duration::from_secs(2),
        "idle await_jobs should block ~the timeout, blocked {idle:?}"
    );

    // (b) wakeup: a concurrent enqueue releases a waiter well before its long timeout.
    let cp2 = cp.clone();
    let kk = k.clone();
    let waiter = tokio::spawn(async move { cp2.await_jobs(&kk, Duration::from_secs(30)).await });
    tokio::time::sleep(Duration::from_millis(100)).await; // let the waiter register / LISTEN
    let t1 = std::time::Instant::now();
    cp.enqueue(NewJob {
        kind: "w".into(),
        payload: serde_json::json!({}),
        run_at: None,
        priority: 0,
    })
    .await
    .expect("enqueue");
    waiter.await.expect("waiter task").expect("await_jobs ok");
    assert!(
        t1.elapsed() < Duration::from_secs(5),
        "enqueue wakeup should beat the 30s timeout, took {:?}",
        t1.elapsed()
    );
}
