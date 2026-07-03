//! The transform job contracts: `OutputMode` wire behavior (lowercase serde,
//! defaults to Append, rejects unknown) and the `TransformJob`/`TypedTransformJob`
//! payload shapes, byte-compatible with the transform service's wire JSON.

use control_plane_core::{
    OutputMode, TRANSFORM_JOB_KIND, TYPED_TRANSFORM_JOB_KIND, TransformJob, TypedTransformJob,
};

#[test]
fn default_is_append() {
    assert_eq!(OutputMode::default(), OutputMode::Append);
}

#[test]
fn deserializes_lowercase_variants() {
    assert_eq!(
        serde_json::from_str::<OutputMode>("\"append\"").unwrap(),
        OutputMode::Append
    );
    assert_eq!(
        serde_json::from_str::<OutputMode>("\"overwrite\"").unwrap(),
        OutputMode::Overwrite
    );
}

#[test]
fn rejects_unknown_value() {
    assert!(serde_json::from_str::<OutputMode>("\"merge\"").is_err());
}

#[test]
fn absent_field_defaults_to_append() {
    #[derive(serde::Deserialize)]
    struct P {
        #[serde(default)]
        output_mode: OutputMode,
    }
    let p: P = serde_json::from_str("{}").unwrap();
    assert_eq!(p.output_mode, OutputMode::Append);
}

#[test]
fn transform_job_parses_todays_wire_json() {
    // today's exact wire JSON parses into the core structs
    let j: TransformJob = serde_json::from_value(serde_json::json!({
        "inputs": [{"schema": "main", "name": "a"}],
        "output": {"schema": "main", "name": "out"},
        "sql": "select 1",
    }))
    .expect("parse");
    assert_eq!(j.output_mode, OutputMode::Append);
    // and serializes back byte-compatibly (output_mode always present when serialized is fine;
    // pin the field VALUES, not absence)
    let v = serde_json::to_value(&j).expect("ser");
    assert_eq!(v["output"]["name"], "out");
    assert_eq!(v["output_mode"], "append");
}

#[test]
fn typed_transform_job_parses_todays_wire_json() {
    let tj: TypedTransformJob = serde_json::from_value(serde_json::json!({
        "inputs": ["Customer"], "output": "Enriched", "sql": "select 1", "output_mode": "overwrite",
    }))
    .expect("parse typed");
    assert_eq!(tj.output_mode, OutputMode::Overwrite);
}

#[test]
fn job_kind_strings_are_wire_frozen() {
    assert_eq!(TRANSFORM_JOB_KIND, "transform");
    assert_eq!(TYPED_TRANSFORM_JOB_KIND, "typed-transform");
}
