//! Proves slice 1's contract: idempotent init, restart-adoption, data
//! persistence, and clean shutdown. Uses the buck `:postgres-bin` via the
//! fixture env (POSTGRES_BIN_DIR / POSTGRES_LD_LIBRARY_PATH / LOOM_MIGRATIONS_DIR).

use std::path::PathBuf;

use managed_postgres::{EmbeddedPg, EmbeddedPgConfig};
use sqlx::postgres::PgPoolOptions;

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
async fn embedded_pg_is_idempotent_and_persistent() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let data = tmp.path().join("pgdata");
    let sock = tmp.path().join("pgrun");
    let migrations =
        PathBuf::from(std::env::var("LOOM_MIGRATIONS_DIR").expect("LOOM_MIGRATIONS_DIR"));

    // 1. Fresh start → initdb ran, db created, migrations applied, write a sentinel.
    let pg = EmbeddedPg::start(cfg(&data, &sock))
        .await
        .expect("first start");
    assert!(data.join("PG_VERSION").exists(), "initdb ran");
    let pool = PgPoolOptions::new()
        .connect_with(pg.connect_options())
        .await
        .expect("connect 1");
    control_plane_postgres::run_migrations(&pool, &migrations)
        .await
        .expect("migrate 1");
    let count1: i64 = sqlx::query_scalar("select count(*) from _sqlx_migrations")
        .fetch_one(&pool)
        .await
        .expect("count migrations 1");
    assert!(count1 > 0, "migrations were applied");
    sqlx::query("insert into acl.subject (id) values ('sentinel')")
        .execute(&pool)
        .await
        .expect("insert sentinel");
    pool.close().await;
    pg.shutdown().await.expect("first shutdown");

    // 2. Restart on the SAME dir → adopt (no re-initdb), zero new migrations, data survives.
    let pg = EmbeddedPg::start(cfg(&data, &sock))
        .await
        .expect("second start");
    let pool = PgPoolOptions::new()
        .connect_with(pg.connect_options())
        .await
        .expect("connect 2");
    control_plane_postgres::run_migrations(&pool, &migrations)
        .await
        .expect("migrate 2");
    let count2: i64 = sqlx::query_scalar("select count(*) from _sqlx_migrations")
        .fetch_one(&pool)
        .await
        .expect("count migrations 2");
    assert_eq!(count1, count2, "no new migrations applied on restart");
    let sentinel: i64 =
        sqlx::query_scalar("select count(*) from acl.subject where id = 'sentinel'")
            .fetch_one(&pool)
            .await
            .expect("read sentinel");
    assert_eq!(sentinel, 1, "data survived restart");
    pool.close().await;
    pg.shutdown().await.expect("second shutdown");
}
