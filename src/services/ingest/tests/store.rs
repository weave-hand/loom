use ingest::store::put;
use object_store::local::LocalFileSystem;

#[tokio::test]
async fn put_writes_under_the_schema_table_key() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalFileSystem::new_with_prefix(dir.path()).unwrap();

    let stored = put(&store, "main/t/loom.parquet", b"hello".to_vec())
        .await
        .unwrap();

    assert_eq!(stored.path, "loom.parquet");
    assert!(stored.path_is_relative);

    let on_disk = std::fs::read(dir.path().join("main").join("t").join("loom.parquet")).unwrap();
    assert_eq!(on_disk, b"hello");
}
