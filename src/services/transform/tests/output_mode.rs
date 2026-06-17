//! `OutputMode` wire contract: lowercase serde, defaults to Append, rejects unknown.

use transform::OutputMode;

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
