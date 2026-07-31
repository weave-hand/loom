//! The `Shutdown` seam: one signal fans out to every serve future, and the drain
//! deadline bounds how long the process waits for in-flight work once it fires.
use std::time::Duration;

use loom_lifecycle::{Shutdown, run_bounded};
use tracing_test::traced_test;

const BOUND: Duration = Duration::from_millis(150);

#[tokio::test]
async fn signalled_resolves_once_the_driving_future_resolves() {
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    let sd = Shutdown::driven_by(async move { drop(rx.await) }, BOUND);

    let a = sd.signalled();
    let b = sd.signalled();
    tx.send(()).unwrap();

    // Every handed-out future resolves — the seam fans one signal out to many.
    tokio::time::timeout(Duration::from_secs(5), async {
        a.await;
        b.await;
    })
    .await
    .expect("signalled() futures did not resolve after the signal fired");
}

#[tokio::test]
async fn the_drain_deadline_does_not_fire_before_the_signal() {
    let sd = Shutdown::driven_by(std::future::pending::<()>(), Duration::from_millis(10));
    // Well past the bound, but the signal never fired, so the deadline must not.
    let early = tokio::time::timeout(Duration::from_millis(200), sd.drain_deadline()).await;
    assert!(early.is_err(), "the drain deadline fired before the signal");
}

#[tokio::test]
async fn run_bounded_returns_the_work_result_when_work_finishes_first() {
    let sd = Shutdown::driven_by(std::future::ready(()), Duration::from_secs(30));
    let out: Result<(), &str> = run_bounded(&sd, Box::pin(async { Err("serve failed") })).await;
    assert_eq!(out, Err("serve failed"), "the work's own result must win");
}

// The bound is only useful if it is loud: an operator seeing a pod exit needs to
// know work was severed rather than drained. `#[traced_test]` captures the span
// for this test; assert on the MESSAGE, not the fn name (which would be trivially
// present — see the note in postgres/tests/fixture_timing.rs).
//
// ATTRIBUTE ORDER MATTERS: `#[tokio::test]` FIRST, `#[traced_test]` SECOND — the
// tracing-test macro must wrap the already-async-transformed fn. Both existing
// users in the tree do it this way (ingest/tests/api_error.rs:108-109,
// query-api/tests/serving_fault_logging.rs); the reverse order does not capture.
#[tokio::test]
#[traced_test]
async fn run_bounded_gives_up_on_stuck_work_after_the_bound_and_says_so() {
    let sd = Shutdown::driven_by(std::future::ready(()), BOUND);
    let started = std::time::Instant::now();

    // Work that never completes — a job wedged past the grace period.
    let out: Result<(), &str> = tokio::time::timeout(
        Duration::from_secs(5),
        run_bounded(&sd, Box::pin(std::future::pending())),
    )
    .await
    .expect("run_bounded hung past its own bound");

    assert_eq!(
        out,
        Ok(()),
        "an expired drain exits cleanly, not with an error"
    );
    assert!(
        started.elapsed() >= BOUND,
        "run_bounded gave up before the bound elapsed: {:?}",
        started.elapsed()
    );
    assert!(
        logs_contain("graceful shutdown timed out"),
        "an expired drain must log loudly that work was severed"
    );
}
