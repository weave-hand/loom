use control_plane_core::{FLUSH_JOB_KIND, FlushJob};

#[test]
fn flush_job_serde_roundtrip_and_kind() {
    assert_eq!(FLUSH_JOB_KIND, "flush_table");
    let j = FlushJob {
        schema: "wh".into(),
        name: "t".into(),
    };
    let v = serde_json::to_value(&j).unwrap();
    assert_eq!(v["schema"], "wh");
    assert_eq!(v["name"], "t");
    let back: FlushJob = serde_json::from_value(v).unwrap();
    assert_eq!(back.schema, "wh");
    assert_eq!(back.name, "t");
}
