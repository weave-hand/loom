//! Backend-agnostic contract tests for the control-plane traits.
//! Each adapter runs the same suite against its own implementation.

use std::time::Duration;

use control_plane_core::{ControlPlane, NewJob, Queue, RetryPolicy};
use time::OffsetDateTime;

/// Verify the transaction seam: read-your-write, commit visibility, rollback
/// discards, and isolation of uncommitted writes. Run by every adapter against a
/// fresh, empty instance.
pub async fn tx_contract<CP: ControlPlane>(cp: &CP) {
    // read-your-write within a tx, then commit is visible to a later tx
    let mut tx = cp.begin().await.expect("begin");
    tx.probe_put("k", 7).await.expect("put");
    assert_eq!(
        tx.probe_get("k").await.expect("get"),
        Some(7),
        "read-your-write"
    );
    tx.commit().await.expect("commit");

    let mut tx2 = cp.begin().await.expect("begin");
    assert_eq!(
        tx2.probe_get("k").await.expect("get"),
        Some(7),
        "visible after commit"
    );
    tx2.rollback().await.expect("rollback");

    // rollback discards staged writes
    let mut tx3 = cp.begin().await.expect("begin");
    tx3.probe_put("r", 1).await.expect("put");
    tx3.rollback().await.expect("rollback");

    let mut tx4 = cp.begin().await.expect("begin");
    assert_eq!(
        tx4.probe_get("r").await.expect("get"),
        None,
        "rollback discarded"
    );
    tx4.rollback().await.expect("rollback");

    // isolation: a concurrent tx does not see another tx's uncommitted writes.
    // (This is read-committed: a concurrent tx's writes become visible once it
    //  commits — snapshot isolation is not required or tested here.)
    let mut a = cp.begin().await.expect("begin");
    a.probe_put("iso", 9).await.expect("put");
    let mut b = cp.begin().await.expect("begin");
    assert_eq!(
        b.probe_get("iso").await.expect("get"),
        None,
        "uncommitted not visible"
    );
    a.commit().await.expect("commit");
    b.rollback().await.expect("rollback");
}

fn job(kind: &str) -> NewJob {
    NewJob {
        kind: kind.into(),
        payload: serde_json::json!({}),
        run_at: None,
        priority: 0,
    }
}

/// Contract for the `Queue` ops (transactional `enqueue` is added in Task 4).
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
}
