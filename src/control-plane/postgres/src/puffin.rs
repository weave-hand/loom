//! Apache Puffin sidecar helpers for loom's vector index. A Puffin file is the
//! standardized Iceberg container for index/stat blobs; loom stores a serialized
//! `FlatIndex` as a single `loom-vector-index-v1` blob. Read/write go through
//! iceberg-rust's `puffin` module over a `FileIO` (object store handle).

use std::collections::HashMap;

use control_plane_core::{ControlPlaneError, FlatIndex, IndexKind, Result, VectorIndex};
use iceberg::io::FileIO;
use iceberg::puffin::{Blob, CompressionCodec, PuffinReader, PuffinWriter};

/// The custom Puffin blob `type` string for loom's flat vector index. Versioned:
/// a future on-disk format bump becomes `loom-vector-index-v2`.
pub const LOOM_VECTOR_INDEX_BLOB_TYPE: &str = "loom-vector-index-v1";

fn be<E: std::fmt::Display>(e: E) -> ControlPlaneError {
    ControlPlaneError::Backend(e.to_string().into())
}

/// Write `payload` as a single `loom-vector-index-v1` blob to a fresh Puffin file
/// at `path`. `field_id` is the Iceberg field id of the indexed vector column;
/// `snapshot_id` is the covered snapshot; `properties` carry self-describing
/// metadata (dim, metric, index-kind, column, identity-column, row-count, …).
pub async fn write_index_blob(
    file_io: &FileIO,
    path: &str,
    payload: &[u8],
    snapshot_id: i64,
    field_id: i32,
    properties: HashMap<String, String>,
) -> Result<()> {
    let output = file_io.new_output(path).map_err(be)?;
    // Footer kept uncompressed; the blob itself uncompressed (the payload is
    // already a compact packed-f32 format — Lz4/Zstd would add a dep surface for
    // little gain in slice 1).
    let mut writer = PuffinWriter::new(&output, HashMap::new(), false)
        .await
        .map_err(be)?;
    let blob = Blob::builder()
        .r#type(LOOM_VECTOR_INDEX_BLOB_TYPE.to_string())
        .fields(vec![field_id])
        .snapshot_id(snapshot_id)
        .sequence_number(0)
        .data(payload.to_vec())
        .properties(properties)
        .build();
    writer.add(blob, CompressionCodec::None).await.map_err(be)?;
    writer.close().await.map_err(be)?;
    Ok(())
}

/// A blob read back from a Puffin file: payload bytes + the footer metadata loom
/// cares about.
pub struct LoadedBlob {
    pub payload: Vec<u8>,
    pub properties: HashMap<String, String>,
    pub snapshot_id: i64,
    pub fields: Vec<i32>,
}

/// Read the single `loom-vector-index-v1` blob from the Puffin file at `path`.
/// Errors if the file has no such blob.
pub async fn read_index_blob(file_io: &FileIO, path: &str) -> Result<LoadedBlob> {
    let input = file_io.new_input(path).map_err(be)?;
    let reader = PuffinReader::new(input);
    let meta = reader.file_metadata().await.map_err(be)?;
    let bm = meta
        .blobs()
        .iter()
        .find(|b| b.blob_type() == LOOM_VECTOR_INDEX_BLOB_TYPE)
        .ok_or_else(|| {
            ControlPlaneError::NotFound(format!("no {LOOM_VECTOR_INDEX_BLOB_TYPE} blob in {path}"))
        })?;
    let blob = reader.blob(bm).await.map_err(be)?;
    Ok(LoadedBlob {
        payload: blob.data().to_vec(),
        properties: blob.properties().clone(),
        snapshot_id: blob.snapshot_id(),
        fields: blob.fields().to_vec(),
    })
}

/// Serialize a `FlatIndex` into a `loom-vector-index-v1` Puffin blob with the
/// self-describing properties the spec mandates.
pub async fn write_flat_index(
    file_io: &FileIO,
    path: &str,
    index: &FlatIndex,
    covered_snapshot: i64,
    field_id: i32,
    column: &str,
    identity_column: &str,
) -> Result<()> {
    let mut props = HashMap::new();
    props.insert("dim".to_string(), index.dim().to_string());
    props.insert("metric".to_string(), index.metric().as_str().to_string());
    props.insert(
        "index-kind".to_string(),
        IndexKind::Flat.as_str().to_string(),
    );
    props.insert("column".to_string(), column.to_string());
    props.insert("identity-column".to_string(), identity_column.to_string());
    props.insert("row-count".to_string(), index.row_count().to_string());
    props.insert("covered-snapshot".to_string(), covered_snapshot.to_string());
    let payload = index.serialize();
    write_index_blob(file_io, path, &payload, covered_snapshot, field_id, props).await
}

/// Read and decode the `FlatIndex` from a `loom-vector-index-v1` Puffin file.
pub async fn read_flat_index(file_io: &FileIO, path: &str) -> Result<FlatIndex> {
    let loaded = read_index_blob(file_io, path).await?;
    FlatIndex::deserialize(&loaded.payload)
}
