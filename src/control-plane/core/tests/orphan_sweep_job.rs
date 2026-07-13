//! The `sweep_orphans` queue kind: it is a known, schedulable kind, its empty
//! payload round-trips `{}`, and `validate_job_schedule` accepts/rejects it.

use control_plane_core::{
    JobSchedule, KNOWN_JOB_KINDS, ORPHAN_SWEEP_JOB_KIND, OrphanSweepJob, SCHEDULABLE_JOB_KINDS,
    validate_job_schedule,
};

#[test]
fn kind_is_known_and_schedulable() {
    assert!(KNOWN_JOB_KINDS.contains(&ORPHAN_SWEEP_JOB_KIND));
    assert!(SCHEDULABLE_JOB_KINDS.contains(&ORPHAN_SWEEP_JOB_KIND));
}

#[test]
fn empty_payload_round_trips_as_object() {
    let v = serde_json::to_value(OrphanSweepJob {}).expect("serialize");
    assert_eq!(v, serde_json::json!({}), "payload is the empty JSON object");
    let _back: OrphanSweepJob = serde_json::from_value(v).expect("deserialize");
}

#[test]
fn schedule_validates_with_empty_payload() {
    let s = JobSchedule {
        name: "nightly-sweep".into(),
        kind: ORPHAN_SWEEP_JOB_KIND.into(),
        payload: serde_json::json!({}),
        cron: "0 4 * * *".into(),
    };
    validate_job_schedule(&s).expect("sweep_orphans schedule must validate");
}

#[test]
fn schedule_rejects_non_object_payload() {
    let s = JobSchedule {
        name: "bad-sweep".into(),
        kind: ORPHAN_SWEEP_JOB_KIND.into(),
        payload: serde_json::json!("not-an-object"),
        cron: "0 4 * * *".into(),
    };
    assert!(
        validate_job_schedule(&s).is_err(),
        "a non-object sweep payload must fail decode validation"
    );
}
