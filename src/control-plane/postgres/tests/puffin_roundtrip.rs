//! De-risk: a `loom-vector-index-v1` Puffin blob round-trips byte-exact
//! (payload + footer metadata) through the pinned iceberg-rust `puffin` module.

use std::collections::HashMap;

use control_plane_core::{FlatIndex, Metric, VectorKey, VectorIndex};
use control_plane_postgres::puffin::{
    LOOM_VECTOR_INDEX_BLOB_TYPE, read_flat_index, read_index_blob, write_flat_index,
    write_index_blob,
};
use iceberg::io::FileIO;

#[tokio::test]
async fn puffin_blob_roundtrips_byte_exact() {
    // A local-filesystem FileIO over a tempdir — no Postgres/S3 needed.
    let dir = tempfile::tempdir().unwrap();
    let file_io = FileIO::new_with_fs();
    let path = format!("{}/idx.puffin", dir.path().display());

    let payload: Vec<u8> = (0u8..200).collect();
    let mut props = HashMap::new();
    props.insert("index-kind".to_string(), "flat".to_string());
    props.insert("dim".to_string(), "4".to_string());
    props.insert("metric".to_string(), "cosine".to_string());

    write_index_blob(&file_io, &path, &payload, 7, 3, props.clone())
        .await
        .unwrap();

    let loaded = read_index_blob(&file_io, &path).await.unwrap();
    assert_eq!(loaded.payload, payload, "payload must round-trip byte-exact");
    assert_eq!(loaded.snapshot_id, 7);
    assert_eq!(loaded.fields, vec![3]);
    assert_eq!(
        loaded.properties.get("index-kind").map(String::as_str),
        Some("flat")
    );
    assert_eq!(
        loaded.properties.get("dim").map(String::as_str),
        Some("4")
    );
    assert_eq!(
        loaded.properties.get("metric").map(String::as_str),
        Some("cosine")
    );
}

/// Confirm the blob type constant has the expected value — a regression guard
/// so a rename/typo never silently changes the on-disk format identifier.
#[test]
fn blob_type_constant_value() {
    assert_eq!(LOOM_VECTOR_INDEX_BLOB_TYPE, "loom-vector-index-v1");
}

#[tokio::test]
async fn flat_index_round_trips_through_puffin() {
    let dir = tempfile::tempdir().unwrap();
    let file_io = FileIO::new_with_fs();
    let path = format!("{}/flat.puffin", dir.path().display());

    let idx = FlatIndex::build(
        4,
        Metric::Cosine,
        vec![
            (VectorKey::Int(10), vec![1.0, 0.0, 0.0, 0.0]),
            (VectorKey::Int(20), vec![0.0, 1.0, 0.0, 0.0]),
        ],
    )
    .unwrap();

    write_flat_index(&file_io, &path, &idx, 5, 3, "embedding", "id")
        .await
        .unwrap();
    let back = read_flat_index(&file_io, &path).await.unwrap();
    assert_eq!(back.dim(), 4);
    assert_eq!(back.metric(), Metric::Cosine);
    assert_eq!(
        back.search(&[1.0, 0.0, 0.0, 0.0], 1)[0].0,
        VectorKey::Int(10)
    );
}
