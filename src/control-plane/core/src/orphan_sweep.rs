//! The orphan-sweep job contract, shared by the producer (a schedule firing) and
//! the consumer (the worker). Lives in core so a zero-pool worker can read it
//! without depending on the postgres adapter. Unlike `gc_table` / `compact_table`,
//! the sweep is **warehouse-scoped**: it names no table — it diffs the whole
//! warehouse — so its payload carries no fields.

/// The queue `kind` for an orphaned-object sweep job.
pub const ORPHAN_SWEEP_JOB_KIND: &str = "sweep_orphans";

/// The payload of a `sweep_orphans` job. A braced (not unit) struct so it
/// round-trips the empty JSON object `{}` that schedules and the queue serialize
/// it as — a unit struct would (de)serialize as `null` instead.
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
#[expect(
    clippy::empty_structs_with_brackets,
    reason = "the schedule/queue payload is the empty JSON object `{}`; a braced struct deserializes it, a unit struct would require `null`"
)]
pub struct OrphanSweepJob {}
