//! Unit coverage for `LOOM_TRANSFORM_BACKEND` parsing (pure logic; no fixtures).

use transform::{TransformBackend, parse_transform_backend};

#[test]
fn unset_or_empty_defaults_to_ducklake() {
    assert_eq!(
        parse_transform_backend(None).unwrap(),
        TransformBackend::DuckLake
    );
    assert_eq!(
        parse_transform_backend(Some("")).unwrap(),
        TransformBackend::DuckLake
    );
}

#[test]
fn explicit_backends_parse_case_insensitively() {
    assert_eq!(
        parse_transform_backend(Some("ducklake")).unwrap(),
        TransformBackend::DuckLake
    );
    assert_eq!(
        parse_transform_backend(Some("Iceberg")).unwrap(),
        TransformBackend::Iceberg
    );
    assert_eq!(
        parse_transform_backend(Some("  ICEBERG ")).unwrap(),
        TransformBackend::Iceberg
    );
}

#[test]
fn unknown_value_errors() {
    let err = parse_transform_backend(Some("parquet")).unwrap_err();
    assert!(err.contains("LOOM_TRANSFORM_BACKEND"), "{err}");
}
