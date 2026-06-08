//! Property-based round-trip for the lineage envelope through the real Postgres
//! adapter: an arbitrary `LineageEvent` (arbitrary payload/inputs/outputs/unicode,
//! every EventType) emitted and read back via `events_for` must compare equal.

use control_plane_core::{DatasetRef, EventType, Lineage, LineageEvent, PageReq, RunId};
use control_plane_postgres::fixture::PgFixture;
use proptest::prelude::*;
use proptest::test_runner::{Config, TestRunner};
use serde_json::Value;
use time::OffsetDateTime;
use uuid::Uuid;

fn event_type() -> impl Strategy<Value = EventType> {
    prop_oneof![
        Just(EventType::Start),
        Just(EventType::Running),
        Just(EventType::Complete),
        Just(EventType::Abort),
        Just(EventType::Fail),
    ]
}

fn dataset_ref() -> impl Strategy<Value = DatasetRef> {
    // Exclude \x00: Postgres TEXT/VARCHAR rejects null bytes via client encoding.
    ("[^\x00]*", "[^\x00]*").prop_map(|(namespace, name)| DatasetRef { namespace, name })
}

/// JSON value with i64-only numbers (floats omitted: jsonb normalizes them and f64
/// equality is fragile). Bounded depth so cases stay small.
fn json_value() -> impl Strategy<Value = Value> {
    let leaf = prop_oneof![
        Just(Value::Null),
        any::<bool>().prop_map(Value::Bool),
        any::<i64>().prop_map(|n| Value::Number(n.into())),
        // Exclude \x00: Postgres jsonb rejects null bytes in JSON string values.
        "[^\x00]*".prop_map(Value::String),
    ];
    leaf.prop_recursive(3, 12, 4, |inner| {
        prop_oneof![
            prop::collection::vec(inner.clone(), 0..4).prop_map(Value::Array),
            prop::collection::hash_map("[^\x00]*", inner, 0..4)
                .prop_map(|m| Value::Object(m.into_iter().collect())),
        ]
    })
}

fn lineage_event() -> impl Strategy<Value = LineageEvent> {
    (
        event_type(),
        // second-granularity time → exact round-trip through timestamptz
        0i64..4_000_000_000,
        prop::collection::vec(dataset_ref(), 0..4),
        prop::collection::vec(dataset_ref(), 0..4),
        json_value(),
    )
        .prop_map(
            |(event_type, secs, inputs, outputs, payload)| LineageEvent {
                run_id: RunId(Uuid::nil()), // overwritten per-case below
                event_type,
                event_time: OffsetDateTime::from_unix_timestamp(secs).unwrap(),
                inputs,
                outputs,
                payload,
            },
        )
}

#[test]
fn lineage_envelope_round_trips() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let fx = PgFixture::start();
    let (cp, _db) = rt.block_on(fx.fresh_db());

    // Bounded case count: each case is one DB round-trip.
    let mut runner = TestRunner::new(Config {
        cases: 16,
        ..Config::default()
    });
    runner
        .run(&lineage_event(), |mut event| {
            // Unique run per case so events_for returns exactly this event.
            event.run_id = RunId(Uuid::new_v4());
            rt.block_on(async {
                cp.emit(event.clone()).await.expect("emit");
                let got = cp
                    .events_for(&event.run_id, PageReq::unbounded())
                    .await
                    .expect("events_for");
                prop_assert_eq!(got.items, vec![event.clone()]);
                Ok(())
            })
        })
        .expect("envelope round-trip property holds");
}
