//! Adopting a data dir whose PG_VERSION major differs from the binary must fail
//! with a clear `VersionMismatch` BEFORE any postmaster spawn — no cryptic crash.

use std::path::PathBuf;

use managed_postgres::{EmbeddedPg, EmbeddedPgConfig, EmbeddedPgError};

fn cfg(data: &std::path::Path, sock: &std::path::Path) -> EmbeddedPgConfig {
    EmbeddedPgConfig {
        bin_dir: PathBuf::from(std::env::var("POSTGRES_BIN_DIR").expect("POSTGRES_BIN_DIR")),
        ld_library_path: std::env::var("POSTGRES_LD_LIBRARY_PATH").unwrap_or_default(),
        data_dir: data.to_path_buf(),
        socket_dir: sock.to_path_buf(),
        database: "loom".to_string(),
    }
}

#[tokio::test]
async fn adopting_a_mismatched_major_fails_clearly() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let data = tmp.path().join("pgdata");
    let sock = tmp.path().join("pgrun");

    // Fresh initdb writes the real major into PG_VERSION; capture it, then shut down.
    let pg = EmbeddedPg::start(cfg(&data, &sock))
        .await
        .expect("first start");
    let real_major: u32 = std::fs::read_to_string(data.join("PG_VERSION"))
        .expect("read PG_VERSION")
        .trim()
        .parse()
        .expect("parse real major");
    pg.shutdown().await.expect("shutdown");

    // Simulate a data dir from an older PG: no modern PostgreSQL major is 1.
    assert_ne!(real_major, 1, "sentinel must differ from the real major");
    std::fs::write(data.join("PG_VERSION"), "1\n").expect("rewrite PG_VERSION");

    // Adopt → typed error, returned before any postmaster spawn.
    let err = EmbeddedPg::start(cfg(&data, &sock))
        .await
        .expect_err("mismatched adopt must fail");
    match err {
        EmbeddedPgError::VersionMismatch {
            data_major,
            binary_major,
        } => {
            assert_eq!(
                data_major, 1,
                "data major read from the (rewritten) PG_VERSION"
            );
            assert_eq!(
                binary_major, real_major,
                "binary major from `postgres --version`"
            );
        }
        other => panic!("expected VersionMismatch, got {other:?}"),
    }
}
