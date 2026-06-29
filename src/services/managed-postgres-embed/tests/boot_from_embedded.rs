//! Proves boot-from-embedded-bytes: extract the embedded PG distribution, then
//! boot EmbeddedPg against the EXTRACTED bin dir (NOT POSTGRES_BIN_DIR) and
//! accept a connection. libxml2 is supplied via the fixture's
//! POSTGRES_LD_LIBRARY_PATH (RE/Arch lack a system .so.2); production relies on
//! the system copy.

use managed_postgres::{EmbeddedPg, EmbeddedPgConfig};
use managed_postgres_embed::extract_pg;
use sqlx::postgres::PgPoolOptions;

#[tokio::test]
async fn boots_from_embedded_distribution() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let cache = tmp.path().join("cache");
    let extracted = extract_pg(&cache).expect("extract");

    // Dist libs from the extraction + the fixture's libxml2 shim.
    let shim = std::env::var("POSTGRES_LD_LIBRARY_PATH").unwrap_or_default();
    // Note: this isolates the BIN dir fully (we use the extracted bin_dir); the
    // dist LIB isolation is partial — a lib missing from the extraction could
    // still resolve from the fixture's postgres-bin/lib. Acceptable for this test.
    let ld = format!("{}:{}", extracted.lib_dir.display(), shim);

    let cfg = EmbeddedPgConfig {
        bin_dir: extracted.bin_dir.clone(),
        ld_library_path: ld,
        data_dir: tmp.path().join("pgdata"),
        socket_dir: tmp.path().join("pgrun"),
        database: "loom".to_string(),
    };

    let pg = EmbeddedPg::start(cfg)
        .await
        .expect("start from embedded dist");
    let pool = PgPoolOptions::new()
        .connect_with(pg.connect_options())
        .await
        .expect("connect");
    let one: i32 = sqlx::query_scalar("select 1")
        .fetch_one(&pool)
        .await
        .expect("select 1");
    assert_eq!(one, 1);
    pool.close().await;
    pg.shutdown().await.expect("shutdown");
}
