//! `Shutdown::install` registers its signal handlers BEFORE it returns. Until a
//! listener exists, SIGTERM's disposition is "terminate the process", so a helper
//! that registers lazily (on the spawned task's first poll) leaves a startup window
//! in which a fast rolling restart kills the process outright.
//!
//! Own test binary on purpose: a sibling test raising SIGTERM on a loop could
//! satisfy the single-raise assertion below by accident.
use std::time::Duration;

/// Raise SIGTERM at this process.
fn raise_sigterm() {
    // SAFETY: raise(3) sends a signal to the calling process and touches no memory;
    // the guard stream below has already displaced the default (terminate) disposition.
    let raised = unsafe { libc::raise(libc::SIGTERM) };
    assert_eq!(raised, 0, "raise(SIGTERM) failed");
}

#[tokio::test]
async fn install_registers_before_it_returns() {
    let guard = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .expect("register the SIGTERM guard stream");

    let sd = loom_lifecycle::Shutdown::install(Duration::from_secs(30));
    let signalled = sd.signalled();

    // Exactly ONE raise, immediately after install returns: if registration were
    // deferred to the spawned task's first poll, this signal would be missed.
    raise_sigterm();

    tokio::time::timeout(Duration::from_secs(5), signalled)
        .await
        .expect("Shutdown::install missed a SIGTERM raised right after it returned");
    drop(guard);
}
