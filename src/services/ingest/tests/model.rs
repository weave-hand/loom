//! Unit tests for `model_shape_from_type`: ObjectType -> conformance ModelShape.

use control_plane_core::{ObjectType, PropertyDef, TableRef, TypeName};
use ingest::{ColumnShape, ModelShape, model_shape_from_type};

fn ty(properties: Vec<PropertyDef>, identity: Option<&str>) -> ObjectType {
    ObjectType {
        name: TypeName("Thing".into()),
        properties,
        derived: vec![],
        table: TableRef {
            schema: "main".into(),
            name: "thing".into(),
        },
        identity: identity.map(Into::into),
    }
}

#[test]
fn maps_one_columnshape_per_property_in_order() {
    let ot = ty(
        vec![
            PropertyDef {
                name: "a".into(),
                ty: "long".into(),
                required: true,
            },
            PropertyDef {
                name: "b".into(),
                ty: "string".into(),
                required: false,
            },
        ],
        None,
    );
    assert_eq!(
        model_shape_from_type(&ot),
        ModelShape {
            columns: vec![
                ColumnShape {
                    name: "a".into(),
                    ty: "long".into(),
                    required: true,
                },
                ColumnShape {
                    name: "b".into(),
                    ty: "string".into(),
                    required: false,
                },
            ],
        }
    );
}

#[test]
fn identity_property_is_required_even_when_property_required_is_false() {
    let ot = ty(
        vec![
            PropertyDef {
                name: "id".into(),
                ty: "long".into(),
                required: false,
            },
            PropertyDef {
                name: "name".into(),
                ty: "string".into(),
                required: false,
            },
        ],
        Some("id"),
    );
    assert_eq!(
        model_shape_from_type(&ot),
        ModelShape {
            columns: vec![
                ColumnShape {
                    name: "id".into(),
                    ty: "long".into(),
                    required: true,
                },
                ColumnShape {
                    name: "name".into(),
                    ty: "string".into(),
                    required: false,
                },
            ],
        }
    );
}
