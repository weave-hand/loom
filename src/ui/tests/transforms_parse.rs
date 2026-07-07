use loom_ui_core::{
    OutputMode, TransformIo, TransformKind, parse_runs, parse_transform_def, parse_transform_list,
};
use serde_json::json;

#[test]
fn list_decodes_kind_schedule_and_flag() {
    let body = json!({
        "transforms": [
            { "name": "daily_rollup", "body": {"kind": "physical"}, "schedule": "0 0 * * *",
              "on_input_commit": false },
            { "name": "enrich_customers", "body": {"kind": "typed"}, "on_input_commit": true },
        ]
    });
    let rows = parse_transform_list(&body);
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].name, "daily_rollup");
    assert_eq!(rows[0].kind, TransformKind::Physical);
    assert_eq!(rows[0].schedule.as_deref(), Some("0 0 * * *"));
    assert!(!rows[0].on_input_commit);
    assert_eq!(rows[1].kind, TransformKind::Typed);
    assert_eq!(rows[1].schedule, None);
    assert!(rows[1].on_input_commit);
}

#[test]
fn list_missing_array_is_empty() {
    assert!(parse_transform_list(&json!({})).is_empty());
}

#[test]
fn def_decodes_physical_body() {
    let body = json!({
        "name": "join_orders",
        "body": {
            "kind": "physical",
            "inputs": [{"schema": "sales", "name": "orders"},
                       {"schema": "sales", "name": "line_items"}],
            "output": {"schema": "marts", "name": "order_totals"},
            "sql": "SELECT 1",
            "output_mode": "overwrite"
        },
        "on_input_commit": true,
        "next_run_at": "2026-07-08T00:00:00Z"
    });
    let def = parse_transform_def(&body);
    assert_eq!(def.name, "join_orders");
    assert_eq!(def.body.kind(), TransformKind::Physical);
    assert_eq!(def.body.sql, "SELECT 1");
    assert_eq!(def.body.output_mode, OutputMode::Overwrite);
    assert!(def.on_input_commit);
    assert_eq!(def.next_run_at.as_deref(), Some("2026-07-08T00:00:00Z"));
    match def.body.io {
        TransformIo::Physical { inputs, output } => {
            assert_eq!(inputs.len(), 2);
            assert_eq!(inputs[0].schema, "sales");
            assert_eq!(inputs[0].name, "orders");
            assert_eq!(output.schema, "marts");
            assert_eq!(output.name, "order_totals");
        }
        TransformIo::Typed { .. } => panic!("expected physical io"),
    }
}

#[test]
fn def_decodes_typed_body_and_defaults_mode_to_append() {
    let body = json!({
        "name": "enrich",
        "body": {
            "kind": "typed",
            "inputs": ["Customer", "Order"],
            "output": "EnrichedCustomer",
            "sql": "SELECT *"
        },
        "on_input_commit": false
    });
    let def = parse_transform_def(&body);
    assert_eq!(def.body.output_mode, OutputMode::Append);
    match def.body.io {
        TransformIo::Typed { inputs, output } => {
            assert_eq!(inputs, vec!["Customer".to_string(), "Order".to_string()]);
            assert_eq!(output, "EnrichedCustomer");
        }
        TransformIo::Physical { .. } => panic!("expected typed io"),
    }
}

#[test]
fn runs_decode_with_optional_fields() {
    let body = json!({
        "runs": [
            { "run_id": "r1", "trigger": "manual", "state": "succeeded",
              "queued_at": "t0", "started_at": "t1", "finished_at": "t2",
              "snapshot_id": "snap-9" },
            { "run_id": "r2", "trigger": "ad-hoc", "state": "failed",
              "queued_at": "t0", "error": "boom" },
        ]
    });
    let runs = parse_runs(&body);
    assert_eq!(runs.len(), 2);
    assert_eq!(runs[0].run_id, "r1");
    assert_eq!(runs[0].state, "succeeded");
    assert_eq!(runs[0].snapshot_id.as_deref(), Some("snap-9"));
    assert_eq!(runs[0].error, None);
    assert_eq!(runs[1].error.as_deref(), Some("boom"));
    assert_eq!(runs[1].started_at, None);
}
