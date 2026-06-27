use control_plane_core::{BUILD_VECTOR_INDEX_JOB_KIND, BuildVectorIndexJob};

#[test]
fn build_vector_index_job_serde_roundtrip_and_kind() {
    assert_eq!(BUILD_VECTOR_INDEX_JOB_KIND, "build_vector_index");
    let j = BuildVectorIndexJob {
        schema: "wh".into(),
        name: "docs".into(),
        column: "embedding".into(),
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
