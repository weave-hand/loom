use service_runtime::{
    Config, ObjectStoreBackend, build_serving_object_store, build_storage_factory,
};
use std::collections::HashMap;

fn base() -> HashMap<String, String> {
    let mut m = HashMap::new();
    m.insert("LOOM_BIND_ADDR".into(), "127.0.0.1:8080".into());
    m.insert("LOOM_DB_HOST".into(), "/sock".into());
    m.insert("LOOM_DB_PORT".into(), "5432".into());
    m.insert("LOOM_DB_USER".into(), "u".into());
    m.insert("LOOM_DB_PASSWORD".into(), "p".into());
    m.insert("LOOM_DB_NAME".into(), "d".into());
    m.insert("LOOM_DATA_PATH".into(), "/data".into());
    m
}

#[test]
fn unset_warehouse_defaults_to_file_uri_and_local_backend() {
    let cfg = Config::from_map(&base()).unwrap();
    assert_eq!(cfg.object_store.warehouse_uri, "file:///data");
    assert!(matches!(
        cfg.object_store.backend,
        ObjectStoreBackend::Local
    ));
}

#[test]
fn explicit_file_uri_is_local() {
    let mut m = base();
    m.insert("LOOM_WAREHOUSE_URI".into(), "file:///warehouse".into());
    let cfg = Config::from_map(&m).unwrap();
    assert!(matches!(
        cfg.object_store.backend,
        ObjectStoreBackend::Local
    ));
}

#[test]
fn s3_uri_with_creds_and_endpoint_parses_path_style() {
    let mut m = base();
    m.insert("LOOM_WAREHOUSE_URI".into(), "s3://warehouse/loom".into());
    m.insert("AWS_ENDPOINT_URL".into(), "http://127.0.0.1:9000".into());
    m.insert("AWS_ACCESS_KEY_ID".into(), "ak".into());
    m.insert("AWS_SECRET_ACCESS_KEY".into(), "sk".into());
    let cfg = Config::from_map(&m).unwrap();
    match cfg.object_store.backend {
        ObjectStoreBackend::S3(s) => {
            assert_eq!(s.bucket, "warehouse");
            assert_eq!(s.endpoint.as_deref(), Some("http://127.0.0.1:9000"));
            assert_eq!(s.region, "us-east-1"); // default when endpoint set
            assert_eq!(s.access_key_id, "ak");
            assert!(s.path_style); // endpoint set => path-style
        }
        _ => panic!("expected S3 backend"),
    }
}

#[test]
fn s3_uri_missing_credentials_is_boot_error() {
    let mut m = base();
    m.insert("LOOM_WAREHOUSE_URI".into(), "s3://warehouse/loom".into());
    // no AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY
    assert!(Config::from_map(&m).is_err());
}

#[test]
fn unknown_scheme_is_invalid() {
    let mut m = base();
    m.insert("LOOM_WAREHOUSE_URI".into(), "gs://bucket/x".into());
    assert!(Config::from_map(&m).is_err());
}

#[test]
fn build_storage_factory_local_for_file_backend() {
    let cfg = Config::from_map(&base()).unwrap();
    let f = build_storage_factory(&cfg.object_store).unwrap();
    assert!(format!("{f:?}").contains("LocalFsStorageFactory"));
    assert!(
        build_serving_object_store(&cfg.object_store)
            .unwrap()
            .is_none()
    );
}

#[test]
fn build_storage_factory_s3_for_s3_backend() {
    let mut m = base();
    m.insert("LOOM_WAREHOUSE_URI".into(), "s3://warehouse/loom".into());
    m.insert("AWS_ENDPOINT_URL".into(), "http://127.0.0.1:9000".into());
    m.insert("AWS_ACCESS_KEY_ID".into(), "ak".into());
    m.insert("AWS_SECRET_ACCESS_KEY".into(), "sk".into());
    let cfg = Config::from_map(&m).unwrap();
    let f = build_storage_factory(&cfg.object_store).unwrap();
    assert!(format!("{f:?}").contains("S3StorageFactory"));
    let (bucket, _store) = build_serving_object_store(&cfg.object_store)
        .unwrap()
        .unwrap();
    assert_eq!(bucket, "warehouse");
}
