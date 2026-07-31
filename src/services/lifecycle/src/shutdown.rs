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

use std::collections::HashMap;
use std::time::Duration;

use loom_config::ConfigError;
use tokio_util::sync::CancellationToken;

/// Default drain bound, in milliseconds. Comfortably under Kubernetes' 30 s
/// default termination grace period, so the process exits on its own terms
/// rather than being SIGKILLed with work in flight.
pub const DEFAULT_SHUTDOWN_TIMEOUT_MS: u64 = 20_000;

/// Read the drain bound from the env snapshot (`LOOM_SHUTDOWN_TIMEOUT_MS`,
/// default [`DEFAULT_SHUTDOWN_TIMEOUT_MS`]). Strict parse: a malformed value
/// fails startup rather than silently falling back to the default.
pub fn shutdown_timeout(vars: &HashMap<String, String>) -> Result<Duration, ConfigError> {
    let mut ms = DEFAULT_SHUTDOWN_TIMEOUT_MS;
    loom_config::overlay_opt(vars, "LOOM_SHUTDOWN_TIMEOUT_MS", &mut ms)?;
    Ok(Duration::from_millis(ms))
}

/// The process's shutdown seam: one signal source fanned out to every serve
/// future, plus the deadline that bounds the drain once the signal fires.
///
/// Cheap to clone (a `CancellationToken` handle and a `Duration`).
#[derive(Clone, Debug)]
pub struct Shutdown {
    token: CancellationToken,
    bound: Duration,
}

impl Shutdown {
    /// Register the SIGINT/SIGTERM handlers and watch for them in a background
    /// task. `bound` is the drain deadline, measured from when the signal fires.
    ///
    /// Registration happens **before this returns**, so a signal arriving in the
    /// first instants of process life is caught rather than killing the process.
    /// Must be called from within a tokio runtime.
    ///
    /// If the handlers cannot be registered the seam degrades to "already
    /// signalled" — the same failure mode as [`shutdown_signal`]: a process that
    /// cannot listen for shutdown drains now rather than refusing to start.
    #[must_use]
    pub fn install(bound: Duration) -> Shutdown {
        match register_signals() {
            Ok(signals) => Shutdown::driven_by(signals, bound),
            Err(e) => {
                // This degradation makes the process drain immediately and exit 0,
                // which looks like a clean, intentional completion under
                // Kubernetes — with nothing else logged, an operator has no way to
                // tell "finished" from "could not listen for SIGTERM." Log it at
                // the same severity as the drain-timeout case so it is not silent.
                tracing::error!(
                    error = %e,
                    "failed to register SIGINT/SIGTERM handlers; shutting down immediately instead of listening for signals"
                );
                Shutdown::driven_by(std::future::ready(()), bound)
            }
        }
    }

    /// The same seam driven by an arbitrary future instead of process signals, so
    /// its semantics are testable without raising real signals.
    #[must_use]
    pub fn driven_by(
        signal: impl Future<Output = ()> + Send + 'static,
        bound: Duration,
    ) -> Shutdown {
        let token = CancellationToken::new();
        let sig = token.clone();
        drop(tokio::spawn(async move {
            signal.await;
            sig.cancel();
        }));
        Shutdown { token, bound }
    }

    /// The underlying token, for components that already take one (the worker's
    /// dispatch loop).
    #[must_use]
    pub fn token(&self) -> CancellationToken {
        self.token.clone()
    }

    /// Resolves when shutdown is signalled. Hand one to each serve future.
    //
    // No `#[must_use]` here: `Future` itself already carries one, so adding a
    // second trips clippy::double_must_use.
    pub fn signalled(&self) -> impl Future<Output = ()> + Send + 'static {
        let token = self.token.clone();
        async move { token.cancelled().await }
    }

    /// Resolves `bound` after the signal fires — never before.
    //
    // No `#[must_use]` here either, for the same reason as `signalled`.
    pub fn drain_deadline(&self) -> impl Future<Output = ()> + Send + 'static {
        let token = self.token.clone();
        let bound = self.bound;
        async move {
            token.cancelled().await;
            tokio::time::sleep(bound).await;
        }
    }

    /// The configured drain bound.
    #[must_use]
    pub fn bound(&self) -> Duration {
        self.bound
    }
}

/// Drive `work` — a serve loop already wired to `sd.signalled()` — to completion,
/// giving up `bound` after the shutdown signal fires.
///
/// `work` arrives boxed: a service's serve future is tens of KiB (it inlines the
/// whole call graph it awaits), and taking it by value would carry all of it in
/// this function's own future frame — and, transitively, in every caller's frame
/// that awaits `run_bounded` — tripping `clippy::large_futures`. A caller builds
/// it with `Box::pin(service::serve(...))`.
///
/// Abandoning wedged work (a job's lease then lapses and reclaim re-runs it) beats
/// being SIGKILLed past the container's grace period, so an expired drain is a
/// clean exit; it logs at ERROR because it means work was severed.
pub async fn run_bounded<E, F>(sd: &Shutdown, work: std::pin::Pin<Box<F>>) -> Result<(), E>
where
    F: Future<Output = Result<(), E>>,
{
    tokio::select! {
        // Prefer a completed drain over a deadline that fired in the same tick.
        biased;
        res = work => res,
        () = sd.drain_deadline() => {
            tracing::error!(
                timeout_ms = u64::try_from(sd.bound().as_millis()).unwrap_or(u64::MAX),
                "graceful shutdown timed out; exiting with work still in flight"
            );
            Ok(())
        }
    }
}
