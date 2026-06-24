//! Pure-logic tests for the S3 storage adapter — key derivation and typetag serde.
//! Real S3 I/O is proven by the hermetic MinIO round-trip (tests/iceberg_s3_roundtrip.rs).
use control_plane_postgres::iceberg_sql_catalog::s3_storage::{S3Storage, S3StorageFactory};
use iceberg::io::StorageFactory;
use std::sync::Arc;

#[test]
fn key_strips_scheme_and_bucket() {
    // s3://warehouse/loom/orders/data/abc.parquet  ->  loom/orders/data/abc.parquet
    let k = S3Storage::key_of("s3://warehouse/loom/orders/data/abc.parquet").unwrap();
    assert_eq!(k.as_ref(), "loom/orders/data/abc.parquet");
}

#[test]
fn key_handles_bucket_root() {
    let k = S3Storage::key_of("s3://warehouse/metadata/v1.json").unwrap();
    assert_eq!(k.as_ref(), "metadata/v1.json");
}

#[test]
fn factory_builds_storage_via_typetag_trait() {
    let f = S3StorageFactory::new(
        "warehouse".into(),
        Some("http://127.0.0.1:9000".into()),
        "us-east-1".into(),
        "minioadmin".into(),
        "minioadmin".into(),
        true,
    );
    // StorageConfig::new() is empty — the factory carries config, not FileIO props.
    let storage = f.build(&iceberg::io::StorageConfig::new()).unwrap();
    assert!(format!("{storage:?}").contains("S3Storage"));
}

#[test]
fn factory_serde_roundtrips_via_typetag() {
    // The trait is #[typetag::serde]; a boxed factory must serialize with a "type" tag.
    let f: Arc<dyn StorageFactory> = Arc::new(S3StorageFactory::new(
        "wh".into(), None, "us-east-1".into(), "ak".into(), "sk".into(), false,
    ));
    let json = serde_json::to_string(&f).unwrap();
    assert!(json.contains("S3StorageFactory"));
    let back: Box<dyn StorageFactory> = serde_json::from_str(&json).unwrap();
    assert!(format!("{back:?}").contains("S3StorageFactory"));
}
