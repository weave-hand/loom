use control_plane_core::{GC_JOB_KIND, GcJob};

#[test]
fn gc_job_serde_roundtrip_and_kind() {
    assert_eq!(GC_JOB_KIND, "gc_table");
    let j = GcJob {
        schema: "wh".into(),
        name: "t".into(),
    };
    let v = serde_json::to_value(&j).unwrap();
    assert_eq!(v["schema"], "wh");
    assert_eq!(v["name"], "t");
    let back: GcJob = serde_json::from_value(v).unwrap();
    assert_eq!(back.schema, "wh");
    assert_eq!(back.name, "t");
}
