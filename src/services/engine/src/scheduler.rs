//! The transform scheduler: the engine's background loop that fires cron
//! schedules. Each pass claims due definitions (the claim itself advances
//! `next_run_at`, so a crash after claiming skips the occurrence rather than
//! double-firing) and submits one `trigger: Schedule` run per claim.

use std::sync::Arc;
use std::time::Duration;

use control_plane_core::{ControlPlane, RunState, RunTrigger, TransformRun};
use time::OffsetDateTime;
use tokio_util::sync::CancellationToken;

/// One scheduler pass; returns how many runs were submitted.
pub async fn tick(cp: &dyn ControlPlane, now: OffsetDateTime, limit: u32) -> usize {
    let due = match cp.transforms().claim_due_schedules(now, limit).await {
        Ok(due) => due,
        Err(e) => {
            tracing::warn!(error = %e, "scheduler: claim_due_schedules failed");
            return 0;
        }
    };
    let mut submitted = 0;
    for def in due {
        let run_id = uuid::Uuid::new_v4();
        let run = TransformRun {
            run_id,
            transform: Some(def.name.clone()),
            trigger: RunTrigger::Schedule,
            state: RunState::Queued,
            body: def.body.clone(),
            queued_at: OffsetDateTime::now_utc(),
            started_at: None,
            finished_at: None,
            snapshot_id: None,
            error: None,
        };
        match cp
            .transforms()
            .submit_run(run, def.body.to_job(run_id))
            .await
        {
            Ok(_) => submitted += 1,
            Err(e) => {
                // The occurrence is skipped (claim already advanced the clock).
                tracing::warn!(transform = %def.name.0, error = %e, "scheduler: submit_run failed");
            }
        }
    }
    submitted
}

/// Tick every `every` until `cancel` fires.
pub async fn scheduler_loop(cp: Arc<dyn ControlPlane>, every: Duration, cancel: CancellationToken) {
    let mut interval = tokio::time::interval(every);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            () = cancel.cancelled() => return,
            _ = interval.tick() => {
                let n = tick(cp.as_ref(), OffsetDateTime::now_utc(), 32).await;
                if n > 0 {
                    tracing::info!(submitted = n, "scheduler: fired due transforms");
                }
            }
        }
    }
}
