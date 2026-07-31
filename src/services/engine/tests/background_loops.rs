//! The engine's background loops stop together when `BackgroundLoops::stop()` is
//! called: both join handles resolve, so neither loop is left ticking after a
//! drain. This test drives `stop()` directly — it does not exercise `run`'s serve
//! path, so it does not by itself prove that a serve *error* still reaches
//! `stop()` rather than skipping it via an early `?`. That ordering (calling
//! `stop()` unconditionally, not gated behind `serve(...).await?`) is enforced by
//! `run`'s structure in `run.rs`, not by this test.
use std::sync::Arc;
use std::time::Duration;

use control_plane_core::ControlPlane;
use control_plane_memory::MemoryControlPlane;
use engine::EngineTuning;
use engine::scheduler::BackgroundLoops;

#[tokio::test]
async fn stop_cancels_and_joins_both_loops() {
    let cp: Arc<dyn ControlPlane> = Arc::new(MemoryControlPlane::new(Duration::from_millis(300)));
    let tuning = EngineTuning::from_map(&std::collections::HashMap::new()).unwrap();

    let loops = BackgroundLoops::spawn(cp, &tuning);

    // `stop` awaits both join handles; a loop that ignored its cancellation token
    // would keep ticking on the default 5s scheduler interval and hang this await.
    tokio::time::timeout(Duration::from_secs(5), loops.stop())
        .await
        .expect("a background loop did not stop when the engine drained");
}
