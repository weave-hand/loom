//! Proves extract_pg unpacks the embedded distribution to a content-addressed
//! cache dir and reuses an existing extraction (idempotent, no re-unpack).
//! Pure file I/O — no Postgres boot — so a plain rust_test (RE-eligible).

use managed_postgres_embed::extract_pg;

#[test]
fn extract_unpacks_then_reuses() {
    let tmp = tempfile::tempdir().expect("tempdir");

    // First call: extracts.
    let first = extract_pg(tmp.path()).expect("first extract");
    assert!(first.bin_dir.join("initdb").exists(), "initdb extracted");
    assert!(
        first.bin_dir.join("postgres").exists(),
        "postgres extracted"
    );
    assert!(first.lib_dir.is_dir(), "lib dir extracted");
    // Cache dir is keyed by version, with no wrapping postgresql-* component.
    assert!(
        first.bin_dir.ends_with("bin"),
        "bin_dir should end in bin, got {:?}",
        first.bin_dir
    );

    let marker = first.bin_dir.join("initdb");
    let mtime_before = std::fs::metadata(&marker).unwrap().modified().unwrap();

    // Second call on the same cache_root: reuses, does not re-unpack.
    let second = extract_pg(tmp.path()).expect("second extract");
    assert_eq!(first.bin_dir, second.bin_dir, "same cache dir reused");
    let mtime_after = std::fs::metadata(&marker).unwrap().modified().unwrap();
    assert_eq!(mtime_before, mtime_after, "no re-unpack on reuse");
}
