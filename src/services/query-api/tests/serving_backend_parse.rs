//! parse_serving_backend: env value -> ServingBackend. `rust_test` (pure logic).

use query_api::serving_datafusion::{ServingBackend, parse_serving_backend};

#[test]
fn defaults_to_ducklake_when_unset() {
    assert_eq!(parse_serving_backend(None), Ok(ServingBackend::DuckLake));
}

#[test]
fn parses_known_values_case_insensitively() {
    assert_eq!(
        parse_serving_backend(Some("ducklake")),
        Ok(ServingBackend::DuckLake)
    );
    assert_eq!(
        parse_serving_backend(Some("iceberg")),
        Ok(ServingBackend::Iceberg)
    );
    assert_eq!(
        parse_serving_backend(Some("ICEBERG")),
        Ok(ServingBackend::Iceberg)
    );
}

#[test]
fn rejects_unknown() {
    assert!(parse_serving_backend(Some("delta")).is_err());
}
