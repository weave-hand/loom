//! Core types for the micro-batch MV slice: the MicroBatch transform body, the
//! stream_mv job payload, the watermark types, and the trigger/validation arms.
//! External rust_test (no inline #[cfg(test)]) — see CLAUDE.md.

use std::collections::HashMap;

use control_plane_core::{
    KNOWN_JOB_KINDS, STREAM_MV_JOB_KIND, StreamMvJob, TableRef, TransformBody, TransformDef,
    TransformName, TriggerNode, mv_key, validate_transform_def,
};

fn tref(schema: &str, name: &str) -> TableRef {
    TableRef {
        schema: schema.into(),
        name: name.into(),
    }
}

fn mv_body() -> TransformBody {
    TransformBody::MicroBatch {
        source: tref("s", "events"),
        output: tref("s", "events_doubled"),
        buckets: 2,
        sql: "select id, val * 2 as dbl from events".into(),
    }
}

#[test]
fn microbatch_body_serde_round_trips_with_tag() {
    let json = serde_json::to_value(mv_body()).expect("serialize");
    assert_eq!(json["kind"], "microbatch", "serde tag");
    let back: TransformBody = serde_json::from_value(json).expect("deserialize");
    assert_eq!(back, mv_body());
}

#[test]
fn to_job_emits_stream_mv_kind_with_frozen_payload() {
    let rid = uuid::Uuid::new_v4();
    let job = mv_body().to_job(rid);
    assert_eq!(job.kind, STREAM_MV_JOB_KIND);
    let payload: StreamMvJob = serde_json::from_value(job.payload).expect("payload decodes");
    assert_eq!(payload.source, tref("s", "events"));
    assert_eq!(payload.output, tref("s", "events_doubled"));
    assert_eq!(payload.buckets, 2);
    assert_eq!(payload.run_id, Some(rid));
    assert!(
        KNOWN_JOB_KINDS.contains(&STREAM_MV_JOB_KIND),
        "dispatchable kind"
    );
}

#[test]
fn trigger_node_resolves_microbatch_io() {
    let node = TriggerNode::resolve(&TransformName("mv".into()), &mv_body(), &HashMap::new());
    assert_eq!(node.inputs, vec![tref("s", "events")]);
    assert_eq!(node.output, Some(tref("s", "events_doubled")));
}

#[test]
fn validate_rejects_degenerate_microbatch_defs() {
    let def = |body| TransformDef {
        name: TransformName("mv".into()),
        body,
        schedule: None,
        on_input_commit: true,
    };
    let bad_buckets = TransformBody::MicroBatch {
        source: tref("s", "a"),
        output: tref("s", "b"),
        buckets: 0,
        sql: "select 1".into(),
    };
    assert!(
        validate_transform_def(&def(bad_buckets)).is_err(),
        "buckets < 1"
    );
    let empty_sql = TransformBody::MicroBatch {
        source: tref("s", "a"),
        output: tref("s", "b"),
        buckets: 1,
        sql: "  ".into(),
    };
    assert!(
        validate_transform_def(&def(empty_sql)).is_err(),
        "empty sql"
    );
    let self_loop = TransformBody::MicroBatch {
        source: tref("s", "a"),
        output: tref("s", "a"),
        buckets: 1,
        sql: "select 1".into(),
    };
    assert!(
        validate_transform_def(&def(self_loop)).is_err(),
        "source == output"
    );
    assert!(
        validate_transform_def(&def(mv_body())).is_ok(),
        "well-formed accepted"
    );
}

#[test]
fn mv_key_is_the_qualified_output_name() {
    assert_eq!(mv_key(&tref("s", "events_doubled")), "s.events_doubled");
}
