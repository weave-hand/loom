//! The `LineageEvent::completed` / `completed_with_run` core constructors: the
//! five-field "completed event" ritual (Complete + empty inputs + now) built once,
//! so the ingest and action emitters stop hand-assembling it.

use control_plane_core::{DatasetRef, EventType, LineageEvent, RunId};
use uuid::Uuid;

fn dref(name: &str) -> DatasetRef {
    DatasetRef {
        namespace: "loom".into(),
        name: name.into(),
    }
}

#[test]
fn completed_sets_complete_empty_inputs_and_carries_outputs_payload() {
    let payload = serde_json::json!({ "source": "test" });
    let ev = LineageEvent::completed(vec![dref("t")], payload.clone());
    assert_eq!(ev.event_type, EventType::Complete);
    assert!(ev.inputs.is_empty());
    assert_eq!(ev.outputs, vec![dref("t")]);
    assert_eq!(ev.payload, payload);
}

#[test]
fn completed_mints_a_distinct_run_id_each_call() {
    let a = LineageEvent::completed(vec![], serde_json::Value::Null);
    let b = LineageEvent::completed(vec![], serde_json::Value::Null);
    assert_ne!(a.run_id, b.run_id, "each completed() mints a fresh run id");
}

#[test]
fn completed_with_run_preserves_the_supplied_run_id() {
    let run = RunId(Uuid::from_u128(42));
    let ev = LineageEvent::completed_with_run(run, vec![dref("out")], serde_json::Value::Null);
    assert_eq!(ev.run_id, run);
    assert_eq!(ev.event_type, EventType::Complete);
    assert!(ev.inputs.is_empty());
    assert_eq!(ev.outputs, vec![dref("out")]);
}
