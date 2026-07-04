//! Serde shapes and helpers of the transforms concern types.

use control_plane_core::{
    OutputMode, RunState, RunTrigger, TRANSFORM_JOB_KIND, TYPED_TRANSFORM_JOB_KIND, TableRef,
    TransformBody, TransformDef, TransformName, validate_transform_def,
};
use uuid::Uuid;

fn physical_body() -> TransformBody {
    TransformBody::Physical {
        inputs: vec![TableRef {
            schema: "main".into(),
            name: "src".into(),
        }],
        output: TableRef {
            schema: "main".into(),
            name: "dst".into(),
        },
        sql: "select * from src".into(),
        output_mode: OutputMode::Append,
    }
}

#[test]
fn body_serde_is_internally_tagged() {
    let v = serde_json::to_value(physical_body()).unwrap();
    assert_eq!(v["kind"], "physical");
    assert_eq!(v["output"]["name"], "dst");
    let back: TransformBody = serde_json::from_value(v).unwrap();
    assert_eq!(back, physical_body());

    let typed: TransformBody = serde_json::from_value(serde_json::json!({
        "kind": "typed", "inputs": ["Order"], "output": "OrderSummary",
        "sql": "select * from Order",
    }))
    .unwrap();
    match typed {
        TransformBody::Typed { output_mode, .. } => assert_eq!(output_mode, OutputMode::Append),
        TransformBody::Physical { .. } => panic!("wrong variant"),
    }
}

#[test]
fn def_serde_defaults_schedule_and_trigger() {
    let def: TransformDef = serde_json::from_value(serde_json::json!({
        "name": "t1",
        "body": {"kind": "typed", "inputs": ["A"], "output": "B", "sql": "select 1"},
    }))
    .unwrap();
    assert_eq!(def.name, TransformName("t1".into()));
    assert_eq!(def.schedule, None);
    assert!(!def.on_input_commit);
}

#[test]
fn to_job_maps_kind_and_threads_run_id() {
    let rid = Uuid::new_v4();
    let job = physical_body().to_job(rid);
    assert_eq!(job.kind, TRANSFORM_JOB_KIND);
    assert_eq!(job.payload["run_id"], serde_json::json!(rid.to_string()));
    assert_eq!(job.payload["output"]["name"], "dst");
    assert!(job.run_at.is_none());
    assert_eq!(job.priority, 0);

    let typed = TransformBody::Typed {
        inputs: vec!["A".into()],
        output: "B".into(),
        sql: "select 1".into(),
        output_mode: OutputMode::Overwrite,
    };
    assert_eq!(typed.to_job(rid).kind, TYPED_TRANSFORM_JOB_KIND);
}

#[test]
fn slice1_rejects_schedule_and_data_trigger() {
    let mut def = TransformDef {
        name: TransformName("t".into()),
        body: physical_body(),
        schedule: Some("* * * * *".into()),
        on_input_commit: false,
    };
    assert!(
        validate_transform_def(&def).is_err(),
        "schedule rejected until slice 2"
    );
    def.schedule = None;
    def.on_input_commit = true;
    assert!(
        validate_transform_def(&def).is_err(),
        "data trigger rejected until slice 3"
    );
    def.on_input_commit = false;
    assert!(validate_transform_def(&def).is_ok());
}

#[test]
fn run_enums_round_trip_strings() {
    for (t, s) in [
        (RunTrigger::Manual, "manual"),
        (RunTrigger::Schedule, "schedule"),
        (RunTrigger::DataTrigger, "data-trigger"),
        (RunTrigger::AdHoc, "ad-hoc"),
    ] {
        assert_eq!(t.as_str(), s);
        assert_eq!(s.parse::<RunTrigger>().unwrap(), t);
    }
    for (t, s) in [
        (RunState::Queued, "queued"),
        (RunState::Running, "running"),
        (RunState::Succeeded, "succeeded"),
        (RunState::Failed, "failed"),
    ] {
        assert_eq!(t.as_str(), s);
        assert_eq!(s.parse::<RunState>().unwrap(), t);
    }
    assert!("bogus".parse::<RunTrigger>().is_err());
    assert!("bogus".parse::<RunState>().is_err());
}

#[test]
fn legacy_payload_without_run_id_still_deserializes() {
    let legacy = serde_json::json!({
        "inputs": [{"schema": "main", "name": "src"}],
        "output": {"schema": "main", "name": "dst"},
        "sql": "select 1",
    });
    let job: control_plane_core::TransformJob = serde_json::from_value(legacy).unwrap();
    assert_eq!(job.run_id, None);
    let typed = serde_json::json!({ "inputs": ["A"], "output": "B", "sql": "select 1" });
    let job: control_plane_core::TypedTransformJob = serde_json::from_value(typed).unwrap();
    assert_eq!(job.run_id, None);
}
