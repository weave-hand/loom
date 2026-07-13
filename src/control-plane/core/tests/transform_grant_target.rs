//! `TransformBody::physical_output_grant_table` returns the physical output table
//! that needs an explicit admin Read grant — `Some` for a `Physical` body, `None`
//! for a `Typed` body (governed by its bound type's grants).

use control_plane_core::{OutputMode, TableRef, TransformBody};

#[test]
fn physical_body_yields_its_output_table() {
    let body = TransformBody::Physical {
        inputs: vec![TableRef {
            schema: "main".into(),
            name: "src".into(),
        }],
        output: TableRef {
            schema: "main".into(),
            name: "dst".into(),
        },
        sql: "select * from src".into(),
        output_mode: OutputMode::default(),
    };
    assert_eq!(
        body.physical_output_grant_table(),
        Some(&TableRef {
            schema: "main".into(),
            name: "dst".into()
        })
    );
}

#[test]
fn typed_body_yields_none() {
    let body = TransformBody::Typed {
        inputs: vec!["Src".into()],
        output: "Dst".into(),
        sql: "select 1".into(),
        output_mode: OutputMode::default(),
    };
    assert_eq!(body.physical_output_grant_table(), None);
}
