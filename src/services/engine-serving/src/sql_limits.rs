//! Per-statement resource bounds for the arbitrary-SQL governed path: the
//! [`GovernedSqlLimits`] budget, the [`DeadlineStream`] wall-clock adapter, and
//! [`governed_stream_error`] — the single classifier that decides whether a
//! DataFusion stream fault was a budget breach.
//!
//! Two mechanisms ship together because neither alone is sufficient: the memory
//! pool cannot stop a pipelined cross-join (it streams forever without ever
//! reserving tracked memory), and the deadline cannot stop a single oversized
//! in-plan allocation before it happens. See issue #664.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion::error::DataFusionError;
use datafusion::physical_plan::{RecordBatchStream, SendableRecordBatchStream};
use futures::Stream;

use crate::serving::EngineServingError;

/// The per-statement budget for one arbitrary-SQL governed execution. `None` on a
/// field means that mechanism is disabled — the documented `0` escape hatch for an
/// operator who needs an unbounded query.
///
/// The budget is per STATEMENT (a fresh `SessionContext` is built per call), not
/// engine-wide: N concurrent statements can still sum to N x the limit. An
/// engine-wide admission-control budget is deliberately out of scope.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GovernedSqlLimits {
    /// Ceiling on the statement's DataFusion memory pool, in bytes.
    pub memory_bytes: Option<usize>,
    /// Wall-clock budget covering the WHOLE query lifetime — catalog registration,
    /// planning, and every batch. A client that stalls mid-stream to pin a plan
    /// open is cut off too; the accepted cost is that a legitimately slow consumer
    /// of a large result can be cut off, mitigated by the generous default.
    pub deadline: Option<Duration>,
}

impl GovernedSqlLimits {
    /// Both mechanisms disabled — the shape every pre-existing governance test
    /// uses, so their assertions about governance stay unchanged.
    #[must_use]
    pub fn unbounded() -> Self {
        Self {
            memory_bytes: None,
            deadline: None,
        }
    }
}

/// Classify a DataFusion fault raised by the governed-SQL path. A memory-pool
/// breach or an elapsed deadline is [`EngineServingError::ResourceExhausted`]
/// (the caller's 429); everything else stays [`EngineServingError::Engine`].
///
/// LOAD-BEARING: `to_serving` is explicitly class-erasing and would bury a pool
/// breach as `Engine`/500. This is the ONE classifier — the eager
/// `execute_stream()` result, the engine's Flight mapping of per-item stream
/// errors, and the tests all route through it, so the class cannot drift.
///
/// `find_root` unwraps `Context`/`Shared` wrappers, so a breach raised deep
/// inside a sort is still recognised.
/// The message NAMES THE BUDGET, per spec. DataFusion's own OOM text is appended for
/// operator diagnosis but cannot stand alone: it advises on DataFusion's own config
/// keys (`datafusion.runtime.memory_limit`, `datafusion.execution.sort_spill_reservation_bytes`
/// — `sorts/sort.rs:753-764`), which mean nothing to a loom caller. That inner text
/// carries operator names and byte counts only, never row values, so it stays safe to
/// echo. The deadline path authors its own message and needs no prefix.
#[must_use]
pub fn governed_stream_error(e: &DataFusionError) -> EngineServingError {
    match e.find_root() {
        DataFusionError::ResourcesExhausted(m) if m.starts_with(DEADLINE_MSG) => {
            EngineServingError::ResourceExhausted(m.clone())
        }
        // "a memory budget", not "its memory budget": with `LOOM_SQL_MEMORY_LIMIT_BYTES=0`
        // the session uses the default unbounded pool, yet a `ResourcesExhausted` can
        // still arrive from another DataFusion budget — naming the disabled knob as the
        // cause would be a confident lie on the documented escape hatch.
        DataFusionError::ResourcesExhausted(m) => EngineServingError::ResourceExhausted(format!(
            "statement exceeded a memory budget (see LOOM_SQL_MEMORY_LIMIT_BYTES): {m}"
        )),
        other => EngineServingError::Engine(other.to_string()),
    }
}

/// The wall-clock breach message. A prefix (not the whole string) so
/// [`governed_stream_error`] can tell a deadline breach from a pool breach and avoid
/// mislabelling one as the other.
pub(crate) const DEADLINE_MSG: &str = "statement exceeded its wall-clock budget";

/// Wall-clock bound on a result stream: every `poll_next` polls a **timer** before
/// delegating, yielding `ResourcesExhausted` once the deadline elapses and then
/// ending the stream.
///
/// An adapter rather than a `tokio::time::timeout` around the whole call because
/// `execute_governed_sql_stream` *returns* a stream that its caller drives — a
/// wrapping future cannot bound work the caller performs later.
///
/// A real `Sleep` (not a bare `Instant::now()` comparison) is load-bearing: the
/// inner stream returns `Pending` while a batch is in flight, and a plain clock
/// check would then not run again until the inner stream itself woke us. Polling
/// the `Sleep` registers OUR waker with the timer, so the budget fires *during* a
/// long in-flight batch. `SortExec` consumes its whole input inside one
/// `poll_next`, so this is exactly the runaway case that matters.
///
/// Honest limit: this cannot preempt a poll that blocks the executor thread in
/// synchronous CPU work without ever yielding. That case is the memory pool's job —
/// which is why the two mechanisms ship together.
pub struct DeadlineStream {
    inner: SendableRecordBatchStream,
    sleep: Pin<Box<tokio::time::Sleep>>,
    /// Latched once the deadline fires so the error is yielded exactly once and
    /// the stream then terminates, rather than erroring forever.
    done: bool,
}

impl DeadlineStream {
    /// Bound `inner` by an ABSOLUTE deadline (not a duration), so the caller can
    /// share one clock across planning and streaming.
    #[must_use]
    pub fn until(inner: SendableRecordBatchStream, deadline: Instant) -> Self {
        Self {
            inner,
            sleep: Box::pin(tokio::time::sleep_until(deadline.into())),
            done: false,
        }
    }
}

impl Stream for DeadlineStream {
    type Item = Result<RecordBatch, DataFusionError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // Every field is `Unpin` (`SendableRecordBatchStream` and the boxed `Sleep`
        // are both `Pin<Box<_>>`), so `get_mut` is sound and needs no projection.
        let this = self.get_mut();
        if this.done {
            return Poll::Ready(None);
        }
        // Poll the timer FIRST: an already-elapsed deadline trips on the first poll,
        // and polling it here is what arms the waker for the `Pending` case below.
        if this.sleep.as_mut().poll(cx).is_ready() {
            this.done = true;
            return Poll::Ready(Some(Err(DataFusionError::ResourcesExhausted(format!(
                "{DEADLINE_MSG} (LOOM_SQL_TIMEOUT_SECS)"
            )))));
        }
        Pin::new(&mut this.inner).poll_next(cx)
    }
}

impl RecordBatchStream for DeadlineStream {
    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }
}
