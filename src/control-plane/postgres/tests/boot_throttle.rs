//! Bound check for the `BootThrottle` file-lock slot semaphore (no cluster).
//! Plain `rust_test` — pure `flock` on temp files, RE-eligible.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::time::Duration;

use control_plane_postgres::fixture::BootThrottle;

#[test]
fn throttle_bounds_concurrent_holders_and_reuses_slots() {
    let dir = tempfile::tempdir().expect("tempdir");
    let throttle = Arc::new(BootThrottle::new(dir.path().to_path_buf(), 2));

    let live = Arc::new(AtomicUsize::new(0));
    let observed_max = Arc::new(AtomicUsize::new(0));
    let completed = Arc::new(AtomicUsize::new(0));

    let mut handles = Vec::new();
    for _ in 0..6 {
        let throttle = Arc::clone(&throttle);
        let live = Arc::clone(&live);
        let observed_max = Arc::clone(&observed_max);
        let completed = Arc::clone(&completed);
        handles.push(thread::spawn(move || {
            let guard = throttle.acquire();
            let now = live.fetch_add(1, Ordering::SeqCst) + 1;
            observed_max.fetch_max(now, Ordering::SeqCst);
            thread::sleep(Duration::from_millis(50));
            live.fetch_sub(1, Ordering::SeqCst);
            completed.fetch_add(1, Ordering::SeqCst);
            drop(guard);
        }));
    }
    for h in handles {
        h.join().expect("thread join");
    }

    assert!(
        observed_max.load(Ordering::SeqCst) <= 2,
        "throttle exceeded its bound: observed max {} > 2",
        observed_max.load(Ordering::SeqCst)
    );
    assert_eq!(
        completed.load(Ordering::SeqCst),
        6,
        "not all threads completed — slots were not reused on drop"
    );
}
