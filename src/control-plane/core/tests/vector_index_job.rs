use control_plane_core::{BUILD_VECTOR_INDEX_JOB_KIND, BuildVectorIndexJob};

#[test]
fn build_vector_index_job_serde_roundtrip_and_kind() {
    assert_eq!(BUILD_VECTOR_INDEX_JOB_KIND, "build_vector_index");
    let j = BuildVectorIndexJob {
        schema: "wh".into(),
        name: "docs".into(),
        column: "embedding".into(),
        index_kind: None,
        nlist: None,
        m: None,
        ef_construction: None,
    };
    let v = serde_json::to_value(&j).unwrap();
    assert_eq!(v["schema"], "wh");
    assert_eq!(v["name"], "docs");
    assert_eq!(v["column"], "embedding");
    let back: BuildVectorIndexJob = serde_json::from_value(v).unwrap();
    assert_eq!(back.schema, "wh");
    assert_eq!(back.name, "docs");
    assert_eq!(back.column, "embedding");
}

#[test]
fn legacy_payload_without_index_fields_deserializes_to_flat() {
    use control_plane_core::IndexSpec;
    // A payload written before IVF existed (no index_kind / nlist keys).
    let v = serde_json::json!({ "schema": "wh", "name": "docs", "column": "embedding" });
    let job: BuildVectorIndexJob = serde_json::from_value(v).unwrap();
    assert_eq!(job.index_kind, None);
    assert_eq!(job.nlist, None);
    assert!(matches!(job.index_spec().unwrap(), IndexSpec::Flat));
}

#[test]
fn ivf_payload_maps_to_ivf_spec() {
    use control_plane_core::IndexSpec;
    let v = serde_json::json!({
        "schema": "wh", "name": "docs", "column": "embedding",
        "index_kind": "ivf_flat", "nlist": 32
    });
    let job: BuildVectorIndexJob = serde_json::from_value(v).unwrap();
    match job.index_spec().unwrap() {
        IndexSpec::IvfFlat { nlist } => assert_eq!(nlist, Some(32)),
        other => panic!("expected IvfFlat, got {other:?}"),
    }
}

#[test]
fn unknown_index_kind_is_error() {
    let v = serde_json::json!({
        "schema": "wh", "name": "docs", "column": "embedding", "index_kind": "bogus"
    });
    let job: BuildVectorIndexJob = serde_json::from_value(v).unwrap();
    assert!(job.index_spec().is_err());
}

#[test]
fn index_spec_from_label_table() {
    use control_plane_core::IndexSpec;
    assert!(matches!(
        IndexSpec::from_label(None, None, None, None).unwrap(),
        IndexSpec::Flat
    ));
    assert!(matches!(
        IndexSpec::from_label(Some("flat"), None, None, None).unwrap(),
        IndexSpec::Flat
    ));
    assert!(matches!(
        IndexSpec::from_label(Some("ivf_flat"), Some(8), None, None).unwrap(),
        IndexSpec::IvfFlat { nlist: Some(8) }
    ));
    assert!(matches!(
        IndexSpec::from_label(Some("hnsw"), None, Some(32), Some(128)).unwrap(),
        IndexSpec::Hnsw {
            m: Some(32),
            ef_construction: Some(128)
        }
    ));
    assert!(IndexSpec::from_label(Some("bogus"), None, None, None).is_err());
}

#[test]
fn hnsw_payload_maps_to_hnsw_spec() {
    use control_plane_core::IndexSpec;
    let v = serde_json::json!({
        "schema": "wh", "name": "docs", "column": "embedding",
        "index_kind": "hnsw", "m": 24, "ef_construction": 100
    });
    let job: BuildVectorIndexJob = serde_json::from_value(v).unwrap();
    match job.index_spec().unwrap() {
        IndexSpec::Hnsw { m, ef_construction } => {
            assert_eq!(m, Some(24));
            assert_eq!(ef_construction, Some(100));
        }
        other => panic!("expected Hnsw, got {other:?}"),
    }
}
