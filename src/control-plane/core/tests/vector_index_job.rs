use control_plane_core::{BUILD_VECTOR_INDEX_JOB_KIND, BuildVectorIndexJob};

#[test]
fn job_round_trips() {
    assert_eq!(BUILD_VECTOR_INDEX_JOB_KIND, "build_vector_index");
    let job = BuildVectorIndexJob {
        schema: "main".into(),
        name: "document".into(),
        index_name: "by_sim".into(),
    };
    let json = serde_json::to_value(&job).unwrap();
    let back: BuildVectorIndexJob = serde_json::from_value(json).unwrap();
    assert_eq!(back.schema, "main");
    assert_eq!(back.name, "document");
    assert_eq!(back.index_name, "by_sim");
}
