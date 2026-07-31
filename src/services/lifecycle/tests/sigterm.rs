//! SIGTERM — the signal container runtimes send — resolves the shared shutdown
//! source. A SIGINT-only test passes against the bug this fixes, so the assertion
//! must be SIGTERM specifically. Raising real signals mutates process-wide state,
//! so this lives in its own `rust_test` target (its own test binary), where it
//! cannot perturb sibling tests.
use std::time::Duration;

/// Raise SIGTERM at this process.
fn raise_sigterm() {
    // SAFETY: raise(3) sends a signal to the calling process and touches no memory;
    // every caller below has already registered a SIGTERM handler, so the default
    // (terminate) disposition is not in force.
    let raised = unsafe { libc::raise(libc::SIGTERM) };
    assert_eq!(raised, 0, "raise(SIGTERM) failed");
}

#[tokio::test]
async fn sigterm_resolves_shutdown_signal() {
    // Register a SIGTERM listener FIRST: tokio replaces the process-killing default
    // disposition as soon as any listener exists, so the raises below cannot kill
    // the test binary even if one lands before the spawned task registers its own.
    let guard = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .expect("register the SIGTERM guard stream");

    let waiter = tokio::spawn(loom_lifecycle::shutdown_signal());

    // `shutdown_signal()` is an async fn: it registers on first poll, so a single
    // raise can legitimately be missed. Re-raise on a short interval.
    let raiser = tokio::spawn(async {
        loop {
            raise_sigterm();
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    });

    tokio::time::timeout(Duration::from_secs(5), waiter)
        .await
        .expect("shutdown_signal did not resolve on SIGTERM within 5s")
        .expect("the shutdown_signal task panicked");

    raiser.abort();
    drop(guard);
}
