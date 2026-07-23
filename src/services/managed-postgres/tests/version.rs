//! Pure parsers for the embedded-Postgres version-skew guard. No cluster.

use managed_postgres::{parse_binary_major, pg_version_major};

#[test]
fn pg_version_major_reads_modern_single_integer() {
    assert_eq!(pg_version_major("17\n"), Some(17));
    assert_eq!(pg_version_major("17"), Some(17));
}

#[test]
fn pg_version_major_takes_major_of_legacy_dotted() {
    // Pre-10 clusters wrote "9.6"; the major for compatibility is the first segment.
    assert_eq!(pg_version_major("9.6\n"), Some(9));
}

#[test]
fn pg_version_major_rejects_garbage() {
    assert_eq!(pg_version_major(""), None);
    assert_eq!(pg_version_major("garbage\n"), None);
}

#[test]
fn parse_binary_major_reads_standard_banner() {
    assert_eq!(
        parse_binary_major("postgres (PostgreSQL) 17.4\n"),
        Some(17)
    );
}

#[test]
fn parse_binary_major_reads_distro_suffix() {
    assert_eq!(
        parse_binary_major("postgres (PostgreSQL) 16.2 (Debian 16.2-1.pgdg120+1)"),
        Some(16)
    );
}

#[test]
fn parse_binary_major_rejects_no_version() {
    assert_eq!(parse_binary_major("nonsense with no number"), None);
    assert_eq!(parse_binary_major(""), None);
}
