//! Unit coverage for the landing-backend selector (pure; no Postgres).

use ingest::landing::{LandingBackend, parse_landing_backend};

#[test]
fn parses_default_and_variants() {
    assert_eq!(
        parse_landing_backend(None).unwrap(),
        LandingBackend::DuckLake,
        "unset defaults to DuckLake"
    );
    assert_eq!(
        parse_landing_backend(Some("")).unwrap(),
        LandingBackend::DuckLake,
        "empty defaults to DuckLake"
    );
    assert_eq!(
        parse_landing_backend(Some("ducklake")).unwrap(),
        LandingBackend::DuckLake
    );
    assert_eq!(
        parse_landing_backend(Some("  ICEBERG  ")).unwrap(),
        LandingBackend::Iceberg,
        "trimmed + case-insensitive"
    );
    assert!(
        parse_landing_backend(Some("delta")).is_err(),
        "unknown backend is rejected"
    );
}
