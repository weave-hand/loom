//! The engine's background loops stop when it drains. `run` owns them as a unit so
//! the serve path cannot cancel one and forget the other — and so a serve *error*
//! cannot skip both, which is what the previous `serve(...).await?`-before-cancel
//! ordering did.
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
