//! Process shutdown: the signal source every loom binary listens on.
//!
//! Container runtimes send **SIGTERM**; an interactive `^C` sends SIGINT. A binary
//! that listens for only one of them is SIGKILLed at the end of its termination
//! grace period, severing in-flight work — the defect this module exists to
//! prevent. One definition, used by all of them.

use std::future::Future;

/// Register the SIGINT and SIGTERM streams **now**, returning a future that
/// resolves when either fires.
///
/// Registration is eager and synchronous on purpose: until a listener exists the
/// kernel's default disposition for SIGTERM is *terminate the process*, so a
/// helper that only registers when its future is first polled leaves a startup
/// window in which a SIGTERM kills the process outright — exactly the failure this
/// module exists to prevent, and a real one during fast rolling restarts.
///
/// Must be called from within a tokio runtime (the signal driver lives there).
pub(crate) fn register_signals() -> std::io::Result<impl Future<Output = ()> + Send + 'static> {
    use tokio::signal::unix::{SignalKind, signal};
    let mut term = signal(SignalKind::terminate())?;
    let mut int = signal(SignalKind::interrupt())?;
    Ok(async move {
        tokio::select! {
            _ = term.recv() => {}
            _ = int.recv() => {}
        }
    })
}

/// Resolve on SIGINT or SIGTERM.
///
/// If the handlers cannot be registered this returns immediately rather than
/// panicking: a process that cannot listen for shutdown degrades to "shut down
/// now" instead of refusing to run.
pub async fn shutdown_signal() {
    if let Ok(signals) = register_signals() {
        signals.await;
    }
}
