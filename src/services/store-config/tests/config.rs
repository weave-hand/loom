use std::collections::HashMap;
use store_config::{ObjectStoreBackend, ObjectStoreConfig, build_write_store};

fn env(pairs: &[(&str, &str)]) -> HashMap<String, String> {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

#[test]
fn parse_from_env_local_and_builds_write_store() {
    let dir = std::env::temp_dir().join("loom-store-config-test");
    std::fs::create_dir_all(&dir).unwrap();
    let uri = format!("file://{}", dir.display());
    let cfg = ObjectStoreConfig::parse_from_env(&env(&[("LOOM_WAREHOUSE_URI", &uri)])).unwrap();
    assert!(matches!(cfg.backend, ObjectStoreBackend::Local));
    let ws = build_write_store(&cfg).unwrap();
    assert_eq!(ws.root_url, uri);
}

#[test]
fn parse_from_env_s3_and_builds_write_store() {
    let cfg = ObjectStoreConfig::parse_from_env(&env(&[
        ("LOOM_WAREHOUSE_URI", "s3://bucket/wh"),
        ("AWS_REGION", "us-east-1"),
        ("AWS_ACCESS_KEY_ID", "k"),
        ("AWS_SECRET_ACCESS_KEY", "s"),
    ]))
    .unwrap();
    assert!(matches!(cfg.backend, ObjectStoreBackend::S3(_)));
    let ws = build_write_store(&cfg).unwrap();
    assert_eq!(ws.root_url, "s3://bucket");
}

#[test]
fn parse_from_env_requires_warehouse_uri() {
    // postgres-free callers have no data_path fallback: missing LOOM_WAREHOUSE_URI errors.
    assert!(ObjectStoreConfig::parse_from_env(&env(&[])).is_err());
}
