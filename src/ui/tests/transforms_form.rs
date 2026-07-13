use loom_ui_core::{OutputMode, TransformForm, TransformKind, form_to_body, form_to_def};
use serde_json::json;

fn physical_form() -> TransformForm {
    TransformForm {
        kind: TransformKind::Physical,
        name: "join_orders".into(),
        inputs: vec!["sales.orders".into(), "sales.line_items".into()],
        output: "marts.order_totals".into(),
        sql: "SELECT 1".into(),
        schedule: String::new(),
        on_input_commit: true,
        output_mode: OutputMode::Overwrite,
    }
}

#[test]
fn physical_def_builds_tagged_json() {
    let def = form_to_def(&physical_form()).expect("valid form");
    assert_eq!(def["name"], json!("join_orders"));
    assert_eq!(def["on_input_commit"], json!(true));
    assert_eq!(def.get("schedule"), None);
    let body = &def["body"];
    assert_eq!(body["kind"], json!("physical"));
    assert_eq!(
        body["inputs"][0],
        json!({"schema": "sales", "name": "orders"})
    );
    assert_eq!(
        body["output"],
        json!({"schema": "marts", "name": "order_totals"})
    );
    assert_eq!(body["output_mode"], json!("overwrite"));
}

#[test]
fn typed_def_builds_string_inputs_and_output() {
    let form = TransformForm {
        kind: TransformKind::Typed,
        name: "enrich".into(),
        inputs: vec!["Customer".into()],
        output: "EnrichedCustomer".into(),
        sql: "SELECT *".into(),
        schedule: "0 0 * * *".into(),
        on_input_commit: false,
        output_mode: OutputMode::Append,
    };
    let def = form_to_def(&form).expect("valid form");
    assert_eq!(def["schedule"], json!("0 0 * * *"));
    let body = &def["body"];
    assert_eq!(body["kind"], json!("typed"));
    assert_eq!(body["inputs"], json!(["Customer"]));
    assert_eq!(body["output"], json!("EnrichedCustomer"));
    assert_eq!(body["output_mode"], json!("append"));
}

#[test]
fn reserved_name_run_is_rejected() {
    let mut form = physical_form();
    form.name = "run".into();
    let errs = form_to_def(&form).expect_err("run is reserved");
    assert!(errs.iter().any(|e| e.field == "name"));
}

#[test]
fn empty_name_and_no_inputs_and_no_sql_all_error() {
    let form = TransformForm {
        kind: TransformKind::Typed,
        ..TransformForm::default()
    };
    let errs = form_to_def(&form).expect_err("empty form");
    assert!(errs.iter().any(|e| e.field == "name"));
    assert!(errs.iter().any(|e| e.field == "inputs"));
    assert!(errs.iter().any(|e| e.field == "output"));
    assert!(errs.iter().any(|e| e.field == "sql"));
}

#[test]
fn physical_inputs_must_be_schema_dot_name() {
    let mut form = physical_form();
    form.inputs = vec!["orders".into()];
    let errs = form_to_def(&form).expect_err("bad input ref");
    assert!(errs.iter().any(|e| e.field == "inputs"));
}

#[test]
fn bad_cron_arity_is_rejected() {
    let mut form = physical_form();
    form.schedule = "0 0 *".into();
    let errs = form_to_def(&form).expect_err("cron arity");
    assert!(errs.iter().any(|e| e.field == "schedule"));
}

#[test]
fn body_builder_skips_name_and_schedule_validation() {
    // Ad-hoc run needs no name; a valid body still passes even with empty name.
    let mut form = physical_form();
    form.name = String::new();
    let body = form_to_body(&form).expect("valid body");
    assert_eq!(body["kind"], json!("physical"));
}
