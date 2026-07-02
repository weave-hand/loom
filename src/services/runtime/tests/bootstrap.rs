//! `service_runtime::bootstrap` over a real (embedded-as-external) Postgres:
//! the migrate-and-exit gate, the Ready context (fields + working pool +
//! structural embedded keep-alive slot), and the fail-loud TTL ordering
//! (TTLs parse BEFORE the pool boots).
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use managed_postgres::{EmbeddedPg, EmbeddedPgConfig};
use service_runtime::{Boot, ConfigError, RuntimeError};
use sqlx::postgres::PgPoolOptions;

// Mirrors tests/migrate_managed.rs (per-file fixture pattern; deliberate).
fn embedded_cfg(data: &Path, sock: &Path) -> EmbeddedPgConfig {
    EmbeddedPgConfig {
        bin_dir: PathBuf::from(std::env::var("POSTGRES_BIN_DIR").expect("POSTGRES_BIN_DIR")),
        ld_library_path: std::env::var("POSTGRES_LD_LIBRARY_PATH").unwrap_or_default(),
        data_dir: data.to_path_buf(),
        socket_dir: sock.to_path_buf(),
        database: "loom".to_string(),
    }
}

/// External-mode env snapshot pointing at `host` (a unix-socket dir).
fn vars_for(host: String, data_path: &Path) -> HashMap<String, String> {
    [
        ("LOOM_BIND_ADDR", "127.0.0.1:0".to_string()),
        ("LOOM_DB_HOST", host),
        ("LOOM_DB_PORT", "5432".to_string()),
        ("LOOM_DB_USER", "postgres".to_string()),
        ("LOOM_DB_PASSWORD", String::new()),
        ("LOOM_DB_NAME", "loom".to_string()),
        ("LOOM_DATA_PATH", data_path.display().to_string()),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_string(), v))
    .collect()
}

async fn loom_schema_present(pg: &EmbeddedPg) -> bool {
    let pool = PgPoolOptions::new()
        .connect_with(pg.connect_options())
        .await
        .expect("connect probe");
    let present: bool = sqlx::query_scalar(
        "select exists(select 1 from information_schema.tables \
         where table_schema = 'acl' and table_name = 'subject')",
    )
    .fetch_one(&pool)
    .await
    .expect("probe query");
    pool.close().await;
    present
}

#[tokio::test]
async fn bootstrap_migrate_mode_applies_schema_and_returns_migrated() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let pg = EmbeddedPg::start(embedded_cfg(
        &tmp.path().join("pgdata"),
        &tmp.path().join("pgrun"),
    ))
    .await
    .expect("start");

    let mut vars = vars_for(pg.socket_dir().to_string_lossy().into_owned(), tmp.path());
    vars.insert("LOOM_MIGRATE".into(), "apply".into());
    let boot = service_runtime::bootstrap(&vars).await.expect("bootstrap");
    assert!(matches!(boot, Boot::Migrated), "expected Boot::Migrated");
    assert!(loom_schema_present(&pg).await, "migrations applied");
    pg.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn bootstrap_ready_builds_the_full_context() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let pg = EmbeddedPg::start(embedded_cfg(
        &tmp.path().join("pgdata"),
        &tmp.path().join("pgrun"),
    ))
    .await
    .expect("start");

    let mut vars = vars_for(pg.socket_dir().to_string_lossy().into_owned(), tmp.path());
    vars.insert("LOOM_SESSION_TTL_SECS".into(), "123".into());
    vars.insert("LOOM_SERVICE_TOKEN_MAX_TTL".into(), "456".into());
    let boot = service_runtime::bootstrap(&vars).await.expect("bootstrap");
    let ctx = match boot {
        Boot::Ready(ctx) => ctx,
        Boot::Migrated => panic!("expected Boot::Ready"),
    };
    assert_eq!(ctx.auth.session_ttl, Duration::from_secs(123));
    assert_eq!(ctx.max_ttl, Duration::from_secs(456));
    assert_eq!(ctx.cfg.db.dbname, "loom");
    let one: i32 = sqlx::query_scalar("select 1")
        .fetch_one(&ctx.pool)
        .await
        .expect("pool works");
    assert_eq!(one, 1);
    ctx.pool.close().await;
    pg.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn bootstrap_malformed_ttl_fails_before_any_pool_build() {
    // No live PG anywhere near this host: if the TTL parse happened AFTER the
    // pool build, this would surface RuntimeError::Pool instead of Config.
    let tmp = tempfile::tempdir().expect("tempdir");
    let mut vars = vars_for(
        tmp.path().join("no-such-pgrun").display().to_string(),
        tmp.path(),
    );
    vars.insert("LOOM_SESSION_TTL_SECS".into(), "soon".into());
    let Err(err) = service_runtime::bootstrap(&vars).await else {
        panic!("malformed TTL must fail bootstrap");
    };
    assert!(
        matches!(err, RuntimeError::Config(ConfigError::Invalid { ref var, .. })
            if var == "LOOM_SESSION_TTL_SECS"),
        "expected Config(Invalid LOOM_SESSION_TTL_SECS), got: {err}"
    );
}
