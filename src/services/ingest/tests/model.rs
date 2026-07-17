//! Unit tests for `model_shape_from_type`: ObjectType -> conformance ModelShape.

use control_plane_core::{ObjectType, PropertyDef};
use ingest::{ColumnShape, ModelShape, model_shape_from_type};

fn ty(properties: Vec<PropertyDef>, identity: Option<&str>) -> ObjectType {
    let builder = properties
        .into_iter()
        .fold(ObjectType::build("Thing", ("main", "thing")), |b, p| {
            b.add_prop(p)
        });
    match identity {
        Some(id) => builder.identity(id).done(),
        None => builder.done(),
    }
}

#[test]
fn maps_one_columnshape_per_property_in_order() {
    let ot = ty(
        vec![
            PropertyDef::new("a", "long").required(),
            PropertyDef::new("b", "string"),
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
                    constraints: control_plane_core::PropertyConstraints::default(),
                },
                ColumnShape {
                    name: "b".into(),
                    ty: "string".into(),
                    required: false,
                    constraints: control_plane_core::PropertyConstraints::default(),
                },
            ],
        }
    );
}

#[test]
fn identity_property_is_required_even_when_property_required_is_false() {
    let ot = ty(
        vec![
            PropertyDef::new("id", "long"),
            PropertyDef::new("name", "string"),
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
                    constraints: control_plane_core::PropertyConstraints::default(),
                },
                ColumnShape {
                    name: "name".into(),
                    ty: "string".into(),
                    required: false,
                    constraints: control_plane_core::PropertyConstraints::default(),
                },
            ],
        }
    );
}
