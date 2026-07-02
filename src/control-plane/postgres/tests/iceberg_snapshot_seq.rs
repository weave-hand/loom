//! `next_snapshot` must hand out distinct, monotonically increasing ids under
//! concurrency — the property the retired `max(snapshot_id)+1` could not give
//! (two callers read the same max and collide on the PK).
use std::collections::HashSet;

use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_mirror::next_snapshot;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn next_snapshot_is_concurrency_safe() {
    let fx = PgFixture::shared();
    let cp = fx.fresh_control_plane().await;
    let pool = cp.pool().clone();

    const N: usize = 32;
    let mut handles = Vec::new();
    for _ in 0..N {
        let pool = pool.clone();
        handles.push(tokio::spawn(async move {
            let mut conn = pool.acquire().await.expect("acquire");
            next_snapshot(&mut conn, None)
                .await
                .expect("next_snapshot")
                .0
        }));
    }
    let mut ids = Vec::new();
    for h in handles {
        ids.push(h.await.expect("join"));
    }
    let unique: HashSet<i64> = ids.iter().copied().collect();
    assert_eq!(unique.len(), N, "snapshot ids must be distinct: {ids:?}");
    assert!(ids.iter().all(|&id| id >= 1), "ids start at 1: {ids:?}");
}
