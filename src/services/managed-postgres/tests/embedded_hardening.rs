//! Proves the slice-1 hardening contract: owner-only dir perms, fast-fail on a
//! dead postmaster, and the single-owner lock. Boots real `initdb`/`postgres`
//! via the buck `:postgres-bin` (fixture env), so it is a `loom_fixture_test`.

use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};

use managed_postgres::{EmbeddedPg, EmbeddedPgConfig, EmbeddedPgError};

fn cfg(data: &Path, sock: &Path) -> EmbeddedPgConfig {
    EmbeddedPgConfig {
        bin_dir: PathBuf::from(std::env::var("POSTGRES_BIN_DIR").expect("POSTGRES_BIN_DIR")),
        ld_library_path: std::env::var("POSTGRES_LD_LIBRARY_PATH").unwrap_or_default(),
        data_dir: data.to_path_buf(),
        socket_dir: sock.to_path_buf(),
        database: "loom".to_string(),
    }
}

fn mode_of(p: &Path) -> u32 {
    std::fs::metadata(p).expect("metadata").permissions().mode() & 0o777
}

#[tokio::test]
async fn data_and_socket_dirs_are_owner_only() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let data = tmp.path().join("pgdata");
    let sock = tmp.path().join("pgrun");
    let pg = EmbeddedPg::start(cfg(&data, &sock)).await.expect("start");
    assert_eq!(mode_of(&data), 0o700, "data dir is owner-only");
    assert_eq!(mode_of(&sock), 0o700, "socket dir is owner-only");
    pg.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn dead_postmaster_fails_fast_not_after_timeout() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let data = tmp.path().join("pgdata");
    let sock = tmp.path().join("pgrun");
    // Boot once to create the cluster, then shut down cleanly.
    EmbeddedPg::start(cfg(&data, &sock))
        .await
        .expect("init")
        .shutdown()
        .await
        .expect("shutdown");
    // Poison the config so the next `postgres` exits immediately on startup.
    let conf = data.join("postgresql.conf");
    let mut body = std::fs::read_to_string(&conf).expect("read conf");
    body.push_str("\nshared_buffers = 'definitely-not-a-size'\n");
    std::fs::write(&conf, body).expect("write conf");

    let t0 = std::time::Instant::now();
    let err = EmbeddedPg::start(cfg(&data, &sock))
        .await
        .expect_err("start must fail on a dead postmaster");
    let elapsed = t0.elapsed();
    assert!(
        matches!(err, EmbeddedPgError::ServerExited(_)),
        "expected ServerExited, got {err:?}"
    );
    // The readiness timeout is 15s; failing well under it proves the fast path.
    assert!(
        elapsed < std::time::Duration::from_secs(10),
        "should fail fast, took {elapsed:?}"
    );
}

#[tokio::test]
async fn second_owner_on_same_data_dir_is_rejected() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let data = tmp.path().join("pgdata");
    let sock = tmp.path().join("pgrun");
    // First owner boots and holds the lock for its lifetime.
    let pg1 = EmbeddedPg::start(cfg(&data, &sock)).await.expect("start 1");
    // A second start on the SAME data dir must be rejected before it can race
    // initdb / a second postmaster.
    let err = EmbeddedPg::start(cfg(&data, &sock))
        .await
        .expect_err("second owner must be rejected");
    assert!(
        matches!(err, EmbeddedPgError::AlreadyLocked(_)),
        "expected AlreadyLocked, got {err:?}"
    );
    // Releasing the first owner frees the lock so a fresh start succeeds.
    pg1.shutdown().await.expect("shutdown 1");
    EmbeddedPg::start(cfg(&data, &sock))
        .await
        .expect("start after release")
        .shutdown()
        .await
        .expect("shutdown 2");
}

#[tokio::test]
async fn drop_without_shutdown_stops_postmaster_and_releases_lock() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let data = tmp.path().join("pgdata");
    let sock = tmp.path().join("pgrun");

    let pid: u32 = {
        let pg = EmbeddedPg::start(cfg(&data, &sock)).await.expect("start");
        // postmaster.pid records the live postmaster pid on its first line.
        let pidfile = std::fs::read_to_string(data.join("postmaster.pid")).expect("pidfile");
        let pid = pidfile
            .lines()
            .next()
            .and_then(|l| l.trim().parse::<u32>().ok())
            .expect("postmaster pid");
        assert!(
            Path::new(&format!("/proc/{pid}")).exists(),
            "postmaster is running before drop"
        );
        // Drop WITHOUT shutdown() → exercises EmbeddedPg::Drop's graceful stop.
        drop(pg);
        pid
    };

    // pg_ctl stop -w waits, so the postmaster should already be gone; poll
    // briefly to absorb any teardown lag without flaking.
    let mut gone = false;
    for _ in 0..100 {
        if !Path::new(&format!("/proc/{pid}")).exists() {
            gone = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    assert!(gone, "postmaster {pid} was stopped by Drop, not left orphaned");

    // The owner lock (sibling of data_dir) is released on drop.
    let mut lock = data.as_os_str().to_os_string();
    lock.push(".loomlock");
    assert!(
        !Path::new(&lock).exists(),
        "owner lockfile was released on drop"
    );
}
