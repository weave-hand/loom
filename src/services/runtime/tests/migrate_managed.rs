//! Managed-branch (external Postgres) migrate wiring: `build_pool_managed` applies
//! the control-plane schema when `migrate_on_boot` is set, and `run_migrations`
//! applies it directly. A fresh `EmbeddedPg` (initdb only — `start()` does NOT
//! migrate) provides the real, empty database; we connect to it as an *external*
//! `DbConfig` over its unix socket, exactly as the chart's services connect to CNPG.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use managed_postgres::{EmbeddedPg, EmbeddedPgConfig};
use service_runtime::{Config, DbConfig};
use sqlx::postgres::PgPoolOptions;
use store_config::ObjectStoreConfig;

// Mirrors managed-postgres/tests/embedded_lifecycle.rs (per-file fixture pattern;
// the duplication is deliberate).
fn embedded_cfg(data: &Path, sock: &Path) -> EmbeddedPgConfig {
    EmbeddedPgConfig {
        bin_dir: PathBuf::from(std::env::var("POSTGRES_BIN_DIR").expect("POSTGRES_BIN_DIR")),
        ld_library_path: std::env::var("POSTGRES_LD_LIBRARY_PATH").unwrap_or_default(),
        data_dir: data.to_path_buf(),
        socket_dir: sock.to_path_buf(),
        database: "loom".to_string(),
    }
}

// An *external*-style DbConfig pointing at the embedded server's unix socket.
// EmbeddedPg connects as user "postgres" over socket_dir; a host beginning with
// "/" is a unix-socket dir (libpq convention) in DbConfig::pg_connect_options.
fn external_db(pg: &EmbeddedPg) -> DbConfig {
    DbConfig {
        host: pg.socket_dir().to_string_lossy().into_owned(),
        port: 5432,
        user: "postgres".to_string(),
        password: String::new(),
        dbname: "loom".to_string(),
        max_connections: Some(4),
    }
}

fn external_config(db: DbConfig, migrate_on_boot: bool, data_path: &Path) -> Config {
    Config {
        bind_addr: "127.0.0.1:0".parse().expect("addr"),
        db,
        data_path: data_path.to_path_buf(),
        object_store: ObjectStoreConfig::parse(&HashMap::new(), data_path)
            .expect("local object store"),
        lock_timeout: Duration::from_millis(5000),
        gc_retention: Duration::from_secs(7 * 24 * 3600),
        embedded: None,
        migrate_on_boot,
    }
}

async fn loom_schema_present(pg: &EmbeddedPg) -> bool {
    let pool = PgPoolOptions::new()
        .connect_with(pg.connect_options())
        .await
        .expect("connect probe");
    // acl.subject exists only after migrations run.
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
async fn build_pool_managed_migrates_on_boot_when_enabled() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let pg = EmbeddedPg::start(embedded_cfg(&tmp.path().join("pgdata"), &tmp.path().join("pgrun")))
        .await
        .expect("start");

    assert!(
        !loom_schema_present(&pg).await,
        "fresh DB has no loom schema"
    );

    let cfg = external_config(external_db(&pg), true, tmp.path());
    let (pool, handle) = service_runtime::build_pool_managed(&cfg)
        .await
        .expect("managed pool");
    assert!(handle.is_none(), "external mode yields no embedded handle");
    pool.close().await;

    assert!(
        loom_schema_present(&pg).await,
        "on-boot migration applied the schema"
    );
    pg.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn build_pool_managed_does_not_migrate_when_disabled() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let pg = EmbeddedPg::start(embedded_cfg(&tmp.path().join("pgdata"), &tmp.path().join("pgrun")))
        .await
        .expect("start");

    let cfg = external_config(external_db(&pg), false, tmp.path());
    let (pool, _handle) = service_runtime::build_pool_managed(&cfg)
        .await
        .expect("managed pool");
    pool.close().await;

    assert!(
        !loom_schema_present(&pg).await,
        "no migration when flag is off"
    );
    pg.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn run_migrations_applies_schema() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let pg = EmbeddedPg::start(embedded_cfg(&tmp.path().join("pgdata"), &tmp.path().join("pgrun")))
        .await
        .expect("start");

    service_runtime::run_migrations(&external_db(&pg))
        .await
        .expect("run_migrations");
    assert!(
        loom_schema_present(&pg).await,
        "run_migrations applied the schema"
    );
    pg.shutdown().await.expect("shutdown");
}
