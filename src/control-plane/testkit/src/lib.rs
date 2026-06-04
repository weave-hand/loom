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
