//! Unit tests for TransformBody::MicroBatchJoin, LookupOn, and the widened
//! StreamMvJob payload. External rust_test (no inline #[cfg(test)]).

use control_plane_core::{
    LookupOn, STREAM_MV_JOB_KIND, StreamMvJob, TableRef, TransformBody, TransformDef,
    TransformName, TriggerNode, validate_transform_def,
};

fn tref(schema: &str, name: &str) -> TableRef {
    TableRef {
        schema: schema.into(),
        name: name.into(),
    }
}

fn join_def(on: Option<LookupOn>) -> TransformDef {
    TransformDef {
        name: TransformName("enrich_orders".into()),
        body: TransformBody::MicroBatchJoin {
            source: tref("s", "orders"),
            enrich: tref("s", "customers"),
            on,
            output: tref("s", "enriched_orders"),
            buckets: 2,
            sql: "SELECT o.id, c.name FROM orders o JOIN customers c ON o.customer_id = c.id"
                .into(),
        },
        schedule: None,
        on_input_commit: true,
    }
}

#[test]
fn microbatch_join_serde_round_trips_with_tag() {
    let def = join_def(Some(LookupOn {
        source_col: "customer_id".into(),
        enrich_col: "id".into(),
    }));
    let json = serde_json::to_value(&def.body).unwrap();
    assert_eq!(json["kind"], "microbatch_join", "serde tag");
    let back: TransformBody = serde_json::from_value(json).unwrap();
    assert_eq!(back, def.body);
}

#[test]
fn to_job_emits_stream_mv_kind_with_enrich_and_on() {
    let def = join_def(Some(LookupOn {
        source_col: "customer_id".into(),
        enrich_col: "id".into(),
    }));
    let run_id = uuid::Uuid::new_v4();
    // `to_job` is a method on `TransformBody`, not `TransformDef` (transforms.rs:75).
    let job = def.body.to_job(run_id);
    assert_eq!(job.kind, STREAM_MV_JOB_KIND, "same kind as slice-4 MVs");
    let payload: StreamMvJob = serde_json::from_value(job.payload).unwrap();
    assert_eq!(payload.enrich, Some(tref("s", "customers")));
    assert_eq!(
        payload.on.as_ref().map(|o| o.source_col.as_str()),
        Some("customer_id")
    );
    assert_eq!(payload.run_id, Some(run_id));
}

#[test]
fn stream_mv_job_without_enrich_keys_decodes_back_compat() {
    // A slice-4 payload (no enrich/on keys) must decode with None/None.
    let slice4 = serde_json::json!({
        "source": {"schema": "s", "name": "a"},
        "output": {"schema": "s", "name": "b"},
        "buckets": 1,
        "sql": "SELECT * FROM a",
        "run_id": null,
    });
    let payload: StreamMvJob = serde_json::from_value(slice4).unwrap();
    assert!(payload.enrich.is_none() && payload.on.is_none());
}

#[test]
fn trigger_node_resolves_source_and_enrich_as_inputs() {
    let def = join_def(None);
    // Real signature (transforms.rs:344): resolve(&TransformName, &TransformBody,
    // &HashMap<String, TableRef>) -> Self (no Result — no `.unwrap()`).
    let node = TriggerNode::resolve(&def.name, &def.body, &std::collections::HashMap::new());
    assert_eq!(
        node.inputs,
        vec![tref("s", "orders"), tref("s", "customers")]
    );
    assert_eq!(node.output, Some(tref("s", "enriched_orders")));
}

#[test]
fn validate_rejects_degenerate_join_defs() {
    let mut cases: Vec<(TransformDef, &str)> = Vec::new();
    let mut d = join_def(None);
    if let TransformBody::MicroBatchJoin { buckets, .. } = &mut d.body {
        *buckets = 0;
    }
    cases.push((d, "buckets < 1"));
    let mut d = join_def(None);
    if let TransformBody::MicroBatchJoin { sql, .. } = &mut d.body {
        sql.clear();
    }
    cases.push((d, "empty sql"));
    let mut d = join_def(None);
    if let TransformBody::MicroBatchJoin { output, .. } = &mut d.body {
        *output = tref("s", "orders");
    }
    cases.push((d, "source == output"));
    let mut d = join_def(None);
    if let TransformBody::MicroBatchJoin { output, .. } = &mut d.body {
        *output = tref("s", "customers");
    }
    cases.push((d, "enrich == output"));
    let mut d = join_def(None);
    if let TransformBody::MicroBatchJoin { enrich, .. } = &mut d.body {
        *enrich = tref("s", "orders");
    }
    cases.push((d, "source == enrich"));
    let mut d = join_def(None);
    if let TransformBody::MicroBatchJoin { enrich, .. } = &mut d.body {
        *enrich = tref("other", "orders");
    }
    cases.push((
        d,
        "registration-name collision (source.name == enrich.name)",
    ));
    let d = join_def(Some(LookupOn {
        source_col: String::new(),
        enrich_col: "id".into(),
    }));
    cases.push((d, "empty LookupOn.source_col"));
    let d = join_def(Some(LookupOn {
        source_col: "customer_id".into(),
        enrich_col: String::new(),
    }));
    cases.push((d, "empty LookupOn.enrich_col"));
    for (def, why) in cases {
        assert!(validate_transform_def(&def).is_err(), "must reject: {why}");
    }
    // The well-formed def validates.
    assert!(validate_transform_def(&join_def(None)).is_ok());
}
