//! LineageWire must losslessly round-trip a LineageEvent (the engine-write RPCs
//! carry the lineage envelope as a JSON string of LineageWire).

use control_plane_core::{DatasetRef, EventType, LineageEvent, RunId};
use engine_wire::convert::LineageWire;
use time::OffsetDateTime;
use uuid::Uuid;

#[test]
fn lineage_event_wire_round_trip() {
    let event = LineageEvent {
        run_id: RunId(Uuid::from_u128(0x1234_5678_9abc_def0_1122_3344_5566_7788)),
        event_type: EventType::Complete,
        // Whole seconds → micros-safe (no precision loss across the i64 micros hop).
        event_time: OffsetDateTime::from_unix_timestamp(1_700_000_000).expect("ts"),
        inputs: vec![DatasetRef {
            namespace: "ns-in".into(),
            name: "a.b.c".into(),
        }],
        outputs: vec![DatasetRef {
            namespace: "ns-out".into(),
            name: "d.e.f".into(),
        }],
        payload: serde_json::json!({ "action": "createWidget", "op": "update" }),
    };

    let wire = LineageWire::from(&event);
    let json = serde_json::to_string(&wire).expect("serialize");
    let parsed: LineageWire = serde_json::from_str(&json).expect("deserialize");
    let back: LineageEvent = parsed.try_into().expect("convert back");

    assert_eq!(event, back);
}

#[test]
fn lineage_wire_rejects_bad_uuid() {
    let wire = LineageWire {
        run_id: "not-a-uuid".into(),
        event_type: "Complete".into(),
        event_time_micros: 0,
        inputs: vec![],
        outputs: vec![],
        payload: serde_json::Value::Null,
    };
    let res: Result<LineageEvent, _> = wire.try_into();
    assert!(res.is_err());
}
