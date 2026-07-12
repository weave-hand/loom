//! Contract tests for `JobSchedule` validation.

use control_plane_core::{ControlPlaneError, JobSchedule, validate_job_schedule};

fn gc_schedule() -> JobSchedule {
    JobSchedule {
        name: "nightly-gc".into(),
        kind: "gc_table".into(),
        payload: serde_json::json!({"schema": "main", "name": "orders"}),
        cron: "0 3 * * *".into(),
    }
}

fn compact_schedule() -> JobSchedule {
    JobSchedule {
        name: "nightly-compact".into(),
        kind: "compact_table".into(),
        payload: serde_json::json!({"schema": "main", "name": "orders"}),
        cron: "0 4 * * *".into(),
    }
}

#[test]
fn accepts_a_good_gc_table_schedule() {
    assert!(validate_job_schedule(&gc_schedule()).is_ok());
}

#[test]
fn accepts_a_good_compact_table_schedule() {
    assert!(validate_job_schedule(&compact_schedule()).is_ok());
}

#[test]
fn rejects_empty_name() {
    let mut s = gc_schedule();
    s.name = String::new();
    let err = validate_job_schedule(&s).unwrap_err();
    assert!(matches!(err, ControlPlaneError::Validation(_)));
}

#[test]
fn rejects_invalid_cron() {
    let mut s = gc_schedule();
    s.cron = "not a cron".into();
    let err = validate_job_schedule(&s).unwrap_err();
    assert!(matches!(err, ControlPlaneError::Validation(_)));
}

#[test]
fn rejects_known_but_unschedulable_kind() {
    let mut s = gc_schedule();
    s.kind = "transform".into();
    let err = validate_job_schedule(&s).unwrap_err();
    assert!(matches!(err, ControlPlaneError::Validation(_)));
}

#[test]
fn rejects_unknown_kind() {
    let mut s = gc_schedule();
    s.kind = "nope".into();
    let err = validate_job_schedule(&s).unwrap_err();
    assert!(matches!(err, ControlPlaneError::Validation(_)));
}

#[test]
fn rejects_gc_table_payload_missing_name() {
    let mut s = gc_schedule();
    s.payload = serde_json::json!({"schema": "main"});
    let err = validate_job_schedule(&s).unwrap_err();
    assert!(matches!(err, ControlPlaneError::Validation(_)));
}
