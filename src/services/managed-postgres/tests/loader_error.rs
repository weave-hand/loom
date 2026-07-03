//! libxml2 fail-fast: a dynamic-loader failure from a pg binary is classified
//! into `MissingSharedLibrary { lib }` (named, points at docs), not a raw
//! `Initdb`/`ServerExited` status. The classifier is a pure function (unit-tested
//! without a broken host); `start`'s preflight is exercised with a stub `postgres`
//! script that exits 127 with a loader error on `-V` — hermetic, no reliance on a
//! host actually lacking the library.

use std::path::{Path, PathBuf};

use managed_postgres::{EmbeddedPg, EmbeddedPgConfig, EmbeddedPgError, classify_loader_error};

#[test]
fn classifies_glibc_loader_error_naming_the_library() {
    let stderr = "/x/bin/postgres: error while loading shared libraries: \
                  libxml2.so.2: cannot open shared object file: No such file or directory\n";
    assert_eq!(
        classify_loader_error(stderr).as_deref(),
        Some("libxml2.so.2")
    );
}

#[test]
fn non_loader_stderr_is_not_classified() {
    assert_eq!(
        classify_loader_error("initdb: error: directory is not empty\n"),
        None
    );
    assert_eq!(classify_loader_error(""), None);
}

/// Write an executable stub named `postgres` under `dir` that prints `stderr_line`
/// to stderr and exits with `code`, regardless of args (so `postgres -V` hits it).
fn stub_postgres(dir: &Path, stderr_line: &str, code: i32) -> PathBuf {
    use std::os::unix::fs::PermissionsExt;
    let path = dir.join("postgres");
    std::fs::write(
        &path,
        format!("#!/bin/sh\necho '{stderr_line}' 1>&2\nexit {code}\n"),
    )
    .expect("write stub");
    let mut perm = std::fs::metadata(&path).expect("meta").permissions();
    perm.set_mode(0o755);
    std::fs::set_permissions(&path, perm).expect("chmod stub");
    path
}

#[tokio::test]
async fn start_preflight_maps_loader_failure_to_named_error() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let bin = tmp.path().join("bin");
    std::fs::create_dir_all(&bin).expect("mkdir bin");
    stub_postgres(
        &bin,
        "postgres: error while loading shared libraries: libxml2.so.2: \
         cannot open shared object file: No such file or directory",
        127,
    );
    let cfg = EmbeddedPgConfig {
        bin_dir: bin,
        ld_library_path: String::new(),
        data_dir: tmp.path().join("pgdata"),
        socket_dir: tmp.path().join("pgrun"),
        database: "loom".to_string(),
    };
    let err = EmbeddedPg::start(cfg)
        .await
        .expect_err("preflight must fail");
    match err {
        EmbeddedPgError::MissingSharedLibrary { ref lib } => assert_eq!(lib, "libxml2.so.2"),
        other => panic!("expected MissingSharedLibrary, got {other:?}"),
    }
    // Message names the library and points operators at the deploy docs.
    let shown = err.to_string();
    assert!(shown.contains("libxml2.so.2"), "names the lib: {shown}");
    assert!(shown.contains("docs/deploy.md"), "points at docs: {shown}");
}
