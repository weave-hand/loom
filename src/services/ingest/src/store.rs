//! Object-store put. `LocalFileSystem` for now (hermetic, no creds); S3 (the
//! object_store `aws` feature) is a later slice. The store is rooted at the
//! catalog's data_path; the key is "<schema>/<table>/<file_name>" so DuckLake's
//! relative-path resolution (data_path + schema.path + table.path + file.path)
//! finds the file. The registered DataFile.path is the file_name alone.

use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, ObjectStoreExt};

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("object store put failed: {0}")]
    Put(#[from] object_store::Error),
}

/// What the caller registers in `append_files`: the file name and its
/// relative-resolution flag.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoredPath {
    pub path: String,
    pub path_is_relative: bool,
}

/// Write `bytes` at `key` (e.g. "main/t/loom.parquet"). Returns the file name to
/// register (the last path segment). Intermediate directories are created by the
/// local store on put.
pub async fn put(
    store: &dyn ObjectStore,
    key: &str,
    bytes: Vec<u8>,
) -> Result<StoredPath, StoreError> {
    store.put(&ObjectPath::from(key), bytes.into()).await?;
    let file_name = key.rsplit('/').next().unwrap_or(key).to_string();
    Ok(StoredPath {
        path: file_name,
        path_is_relative: true,
    })
}
