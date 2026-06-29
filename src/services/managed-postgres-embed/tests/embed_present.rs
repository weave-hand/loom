//! Proves the PG distribution is actually baked into the binary (the
//! $(location)+env+include_bytes! wiring works) and the version env resolves.

#[test]
fn pg_tarball_is_embedded() {
    let len = managed_postgres_embed::pg_tarball_len();
    assert!(
        len > 10_000_000,
        "expected the embedded PG tarball to be >10MB, got {len} bytes"
    );
}

#[test]
fn pg_version_env_resolves() {
    assert_eq!(managed_postgres_embed::PG_VERSION, "17.9.0");
}
