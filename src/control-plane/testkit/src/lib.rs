//! Backend-agnostic contract tests for the control-plane traits.
//! Each adapter runs the same suite against its own implementation.

use control_plane_core::ControlPlane;

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
