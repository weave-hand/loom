//! `TransformBody::output_grant_table` returns the bare physical output table that
//! needs an explicit admin Read grant for the output to be visible in the gated
//! catalog / lineage. `Some` for every untyped-output body — `Physical` and both
//! micro-batch (materialized-view) variants — and `None` for a `Typed` body, whose
//! output visibility rides its bound ontology type's grants.

use control_plane_core::{LookupOn, OutputMode, TableRef, TransformBody};

fn tref(schema: &str, name: &str) -> TableRef {
    TableRef {
        schema: schema.into(),
        name: name.into(),
    }
}

#[test]
fn physical_body_yields_its_output_table() {
    let body = TransformBody::Physical {
        inputs: vec![tref("main", "src")],
        output: tref("main", "dst"),
        sql: "select * from src".into(),
        output_mode: OutputMode::default(),
    };
    assert_eq!(body.output_grant_table(), Some(&tref("main", "dst")));
}

#[test]
fn typed_body_yields_none() {
    let body = TransformBody::Typed {
        inputs: vec!["Src".into()],
        output: "Dst".into(),
        sql: "select 1".into(),
        output_mode: OutputMode::default(),
    };
    assert_eq!(body.output_grant_table(), None);
}

#[test]
fn microbatch_body_yields_its_output_table() {
    let body = TransformBody::MicroBatch {
        source: tref("main", "events"),
        output: tref("main", "rollup"),
        buckets: 4,
        sql: "select 1".into(),
    };
    assert_eq!(body.output_grant_table(), Some(&tref("main", "rollup")));
}

#[test]
fn microbatch_join_body_yields_its_output_table() {
    let body = TransformBody::MicroBatchJoin {
        source: tref("main", "events"),
        enrich: tref("main", "users"),
        on: Some(LookupOn {
            source_col: "user_id".into(),
            enrich_col: "id".into(),
        }),
        output: tref("main", "enriched"),
        buckets: 2,
        sql: "select 1".into(),
    };
    assert_eq!(body.output_grant_table(), Some(&tref("main", "enriched")));
}
